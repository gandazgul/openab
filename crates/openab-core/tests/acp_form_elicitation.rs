use async_trait::async_trait;
use openab_core::acp::connection::AcpConnection;
use openab_core::acp::elicitation::{
    ElicitationContext, ElicitationOutcome, ElicitationPresentation, ElicitationStatus,
    FormPresenter,
};
use openab_core::acp::ContentBlock;
use openab_core::acp::SessionPool;
use openab_core::adapter::{AdapterRouter, ChannelRef, ChatAdapter, MessageContext, MessageRef};
use openab_core::config::{AgentConfig, ReactionsConfig};
use openab_core::markdown::TableMode;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

#[derive(Clone)]
struct RecordingPresenter {
    seen_tx: mpsc::UnboundedSender<ElicitationPresentation>,
    outcome_rx: Arc<Mutex<mpsc::UnboundedReceiver<ElicitationOutcome>>>,
    expired_tx: mpsc::UnboundedSender<ElicitationStatus>,
}

#[async_trait]
impl FormPresenter for RecordingPresenter {
    async fn present_form(
        &self,
        presentation: ElicitationPresentation,
    ) -> anyhow::Result<ElicitationOutcome> {
        self.seen_tx.send(presentation).unwrap();
        self.outcome_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("test outcome channel closed"))
    }

    async fn expire_form(&self, _nonce: &str, status: ElicitationStatus) {
        let _ = self.expired_tx.send(status);
    }
}

fn channel() -> ChannelRef {
    ChannelRef {
        platform: "discord".into(),
        channel_id: "10".into(),
        thread_id: None,
        parent_id: None,
        origin_event_id: None,
    }
}

fn trigger_message() -> MessageRef {
    MessageRef {
        channel: channel(),
        message_id: "99".into(),
    }
}

struct TestAdapter {
    presenter: Arc<dyn FormPresenter>,
    sent_tx: mpsc::UnboundedSender<String>,
}

#[async_trait]
impl ChatAdapter for TestAdapter {
    fn platform(&self) -> &'static str {
        "discord"
    }

    fn message_limit(&self) -> usize {
        2000
    }

    fn form_presenter(&self) -> Option<Arc<dyn FormPresenter>> {
        Some(self.presenter.clone())
    }

    async fn send_message(
        &self,
        channel: &ChannelRef,
        content: &str,
    ) -> anyhow::Result<MessageRef> {
        let _ = self.sent_tx.send(content.to_string());
        Ok(MessageRef {
            channel: channel.clone(),
            message_id: "sent".to_string(),
        })
    }

    async fn create_thread(
        &self,
        channel: &ChannelRef,
        _trigger_msg: &MessageRef,
        _title: &str,
    ) -> anyhow::Result<ChannelRef> {
        Ok(channel.clone())
    }

    async fn add_reaction(&self, _msg: &MessageRef, _emoji: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn remove_reaction(&self, _msg: &MessageRef, _emoji: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn use_streaming(&self, _other_bot_present: bool) -> bool {
        false
    }
}

fn fake_agent_config(scenario: &str) -> anyhow::Result<AgentConfig> {
    let exe = std::env::current_exe()?;
    let mut env = HashMap::new();
    env.insert("OPENAB_FAKE_AGENT".to_string(), "1".to_string());
    env.insert("OPENAB_FAKE_SCENARIO".to_string(), scenario.to_string());
    Ok(AgentConfig {
        command: exe.to_string_lossy().to_string(),
        args: vec![
            "--exact".to_string(),
            "fake_agent_entry".to_string(),
            "--nocapture".to_string(),
        ],
        working_dir: ".".to_string(),
        env,
        inherit_env: vec![],
        command_explicit: true,
    })
}

fn test_reactions_config() -> ReactionsConfig {
    ReactionsConfig {
        enabled: false,
        ..Default::default()
    }
}

async fn spawn_fake(
    scenario: &str,
    presenter: Option<Arc<dyn FormPresenter>>,
) -> anyhow::Result<AcpConnection> {
    let exe = std::env::current_exe()?;
    let args = vec![
        "--exact".to_string(),
        "fake_agent_entry".to_string(),
        "--nocapture".to_string(),
    ];
    let mut env = HashMap::new();
    env.insert("OPENAB_FAKE_AGENT".to_string(), "1".to_string());
    env.insert("OPENAB_FAKE_SCENARIO".to_string(), scenario.to_string());
    AcpConnection::spawn_with_elicitation(exe.to_str().unwrap(), &args, ".", &env, &[], presenter)
        .await
}

#[tokio::test]
async fn fake_runtime_observes_discord_form_capability_and_completes_prompt() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("accept_collision", Some(presenter))
        .await
        .unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (mut rx, request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string(), "user-b".to_string()]),
            }),
        )
        .await
        .unwrap();

    let presentation = tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(presentation.agent_request_id.as_u64(), Some(request_id));
    assert!(presentation.authorized_user_ids.contains("user-a"));
    assert_eq!(presentation.form.fields.len(), 2);

    let pending = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.method.as_deref(), Some("session/update"));
    assert_eq!(
        pending
            .params
            .as_ref()
            .and_then(|p| p.pointer("/update/content/text"))
            .and_then(Value::as_str),
        Some("pending")
    );

    let mut content = presentation.form.default_content();
    content.insert("strategy".into(), json!("balanced"));
    content.insert("confirm".into(), json!(true));
    outcome_tx
        .send(ElicitationOutcome::Accept(content))
        .unwrap();

    loop {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if msg
            .id
            .as_ref()
            .and_then(openab_core::acp::protocol::JsonRpcId::as_u64)
            == Some(request_id)
        {
            assert_eq!(
                msg.result
                    .as_ref()
                    .and_then(|r| r.get("stopReason"))
                    .and_then(Value::as_str),
                Some("end_turn")
            );
            break;
        }
    }
    assert_eq!(expired_rx.recv().await, Some(ElicitationStatus::Submitted));
    conn.prompt_done().await;
}

#[tokio::test]
async fn cancel_response_keeps_string_request_id_on_wire() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("cancel_string_id", Some(presenter))
        .await
        .unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (mut rx, request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string()]),
            }),
        )
        .await
        .unwrap();

    let presentation = tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        presentation.agent_request_id,
        openab_core::acp::protocol::JsonRpcId::String(ref id) if id == "elicitation-a"
    ));
    outcome_tx.send(ElicitationOutcome::Cancel).unwrap();

    loop {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if msg
            .id
            .as_ref()
            .and_then(openab_core::acp::protocol::JsonRpcId::as_u64)
            == Some(request_id)
        {
            break;
        }
    }
    assert_eq!(expired_rx.recv().await, Some(ElicitationStatus::Cancelled));
    conn.prompt_done().await;
}

#[cfg(unix)]
#[tokio::test]
async fn failed_decline_delivery_expires_form() {
    assert_failed_delivery_expires_form(ElicitationOutcome::Decline).await;
}

#[cfg(unix)]
#[tokio::test]
async fn failed_accept_delivery_expires_form() {
    assert_failed_delivery_expires_form(ElicitationOutcome::Accept(serde_json::Map::from_iter([
        ("name".to_string(), json!("Ada")),
    ])))
    .await;
}

#[cfg(unix)]
async fn assert_failed_delivery_expires_form(outcome: ElicitationOutcome) {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("closed_stdin", Some(presenter)).await.unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (_rx, _request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string()]),
            }),
        )
        .await
        .unwrap();

    let presentation = tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        presentation.agent_request_id,
        openab_core::acp::protocol::JsonRpcId::String(ref id) if id == "elicitation-a"
    ));
    outcome_tx.send(outcome).unwrap();

    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), expired_rx.recv())
            .await
            .unwrap(),
        Some(ElicitationStatus::Expired)
    );
    conn.prompt_done().await;
}

#[tokio::test]
async fn cancel_response_keeps_negative_request_id_on_wire() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("cancel_negative_id", Some(presenter))
        .await
        .unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (mut rx, request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string()]),
            }),
        )
        .await
        .unwrap();

    let presentation = tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        presentation.agent_request_id,
        openab_core::acp::protocol::JsonRpcId::Number(-1)
    ));
    outcome_tx.send(ElicitationOutcome::Cancel).unwrap();

    loop {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if msg
            .id
            .as_ref()
            .and_then(openab_core::acp::protocol::JsonRpcId::as_u64)
            == Some(request_id)
        {
            break;
        }
    }
    assert_eq!(expired_rx.recv().await, Some(ElicitationStatus::Cancelled));
    conn.prompt_done().await;
}

#[tokio::test]
async fn unsupported_sessions_do_not_advertise_elicitation() {
    let mut conn = spawn_fake("no_capability", None).await.unwrap();
    conn.initialize().await.unwrap();
}

#[tokio::test]
async fn abandon_request_invalidates_pending_elicitation() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (_outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("accept_collision", Some(presenter))
        .await
        .unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (_rx, request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string()]),
            }),
        )
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    conn.abandon_request(request_id).await;
    assert_eq!(expired_rx.recv().await, Some(ElicitationStatus::Cancelled));
    assert_eq!(conn.pending_elicitations().await, 0);
}

#[tokio::test]
async fn prompt_deadline_cleanup_invalidates_pending_elicitation() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (_outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });
    let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
    let adapter: Arc<dyn ChatAdapter> = Arc::new(TestAdapter { presenter, sent_tx });
    let pool = Arc::new(SessionPool::new(
        fake_agent_config("deadline_hang").unwrap(),
        1,
        30,
        HashMap::new(),
    ));
    let router = AdapterRouter::new(
        pool.clone(),
        test_reactions_config(),
        TableMode::Off,
        1,
        1,
        HashMap::new(),
        std::path::PathBuf::from("."),
    );

    router
        .handle_message(
            &adapter,
            MessageContext {
                thread_channel: channel(),
                sender_json: json!({"sender_id":"user-a"}).to_string(),
                prompt: "hello".to_string(),
                extra_blocks: vec![],
                trigger_msg: trigger_message(),
                other_bot_present: false,
            },
        )
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired_rx.recv().await, Some(ElicitationStatus::Cancelled));
    let warning = tokio::time::timeout(std::time::Duration::from_secs(2), sent_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(warning.contains("Agent exceeded hard timeout"));
    let pending = pool
        .with_connection("discord:10", |conn| {
            Box::pin(async move { Ok(conn.pending_elicitations().await) })
        })
        .await
        .unwrap();
    assert_eq!(pending, 0);
}

async fn pool_with_pending_elicitation(
    scenario: &str,
) -> (
    Arc<SessionPool>,
    mpsc::UnboundedReceiver<ElicitationPresentation>,
    mpsc::UnboundedReceiver<ElicitationStatus>,
    mpsc::UnboundedSender<ElicitationOutcome>,
) {
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });
    let pool = Arc::new(SessionPool::new(
        fake_agent_config(scenario).unwrap(),
        1,
        30,
        HashMap::new(),
    ));
    pool.get_or_create("discord:10", None, Some(presenter))
        .await
        .unwrap();
    pool.with_connection("discord:10", |conn| {
        Box::pin(async move {
            conn.session_prompt(
                vec![ContentBlock::Text {
                    text: "hello".into(),
                }],
                Some(ElicitationContext {
                    channel: channel(),
                    trigger_message: trigger_message(),
                    authorized_user_ids: HashSet::from(["user-a".to_string()]),
                }),
            )
            .await
            .map(|_| ())
        })
    })
    .await
    .unwrap();
    (pool, seen_rx, expired_rx, outcome_tx)
}

#[tokio::test]
async fn capability_mismatch_gives_reset_hint_without_session_key() {
    let (pool, mut seen_rx, _expired_rx, _outcome_tx) =
        pool_with_pending_elicitation("deadline_cancel").await;
    tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let error = pool
        .get_or_create("discord:10", None, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("/reset"));
    assert!(!error.contains("discord"));
    assert!(!error.contains("10"));
    pool.reset_session("discord:10").await.unwrap();
}

#[tokio::test]
async fn reset_session_invalidates_pending_elicitation() {
    let (pool, mut seen_rx, mut expired_rx, _outcome_tx) =
        pool_with_pending_elicitation("deadline_cancel").await;

    tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    pool.reset_session("discord:10").await.unwrap();
    // Cleanup can close the child before the Cancel response is delivered.
    // A closed response pipe must report Expired, never successful delivery.
    assert!(matches!(
        expired_rx.recv().await,
        Some(ElicitationStatus::Cancelled | ElicitationStatus::Expired)
    ));
}

#[tokio::test]
async fn idle_eviction_invalidates_pending_elicitation() {
    let (pool, mut seen_rx, mut expired_rx, _outcome_tx) =
        pool_with_pending_elicitation("deadline_cancel").await;

    tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    pool.with_connection("discord:10", |conn| {
        Box::pin(async move {
            conn.last_active = tokio::time::Instant::now() - std::time::Duration::from_secs(1);
            Ok(())
        })
    })
    .await
    .unwrap();
    pool.cleanup_idle(0).await;
    // Cleanup can close the child before the Cancel response is delivered.
    // A closed response pipe must report Expired, never successful delivery.
    assert!(matches!(
        expired_rx.recv().await,
        Some(ElicitationStatus::Cancelled | ElicitationStatus::Expired)
    ));
}

#[tokio::test]
async fn agent_eof_invalidates_pending_elicitation() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (_outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, mut expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("eof_pending", Some(presenter)).await.unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (mut rx, _request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string()]),
            }),
        )
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .is_none()
    );
    // Cleanup can close the child before the Cancel response is delivered.
    // A closed response pipe must report Expired, never successful delivery.
    assert!(matches!(
        expired_rx.recv().await,
        Some(ElicitationStatus::Cancelled | ElicitationStatus::Expired)
    ));
    assert_eq!(conn.pending_elicitations().await, 0);
}

#[tokio::test]
async fn excess_reverse_requests_get_bounded_errors_while_prompt_stays_live() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (expired_tx, _expired_rx) = mpsc::unbounded_channel();
    let presenter = Arc::new(RecordingPresenter {
        seen_tx,
        outcome_rx: Arc::new(Mutex::new(outcome_rx)),
        expired_tx,
    });

    let mut conn = spawn_fake("overload", Some(presenter)).await.unwrap();
    conn.initialize().await.unwrap();
    conn.session_new(".").await.unwrap();
    let (mut rx, request_id) = conn
        .session_prompt(
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            Some(ElicitationContext {
                channel: channel(),
                trigger_message: trigger_message(),
                authorized_user_ids: HashSet::from(["user-a".to_string()]),
            }),
        )
        .await
        .unwrap();

    let presentation = tokio::time::timeout(std::time::Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), seen_rx.recv())
            .await
            .is_err()
    );
    outcome_tx.send(ElicitationOutcome::Decline).unwrap();

    loop {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if msg
            .id
            .as_ref()
            .and_then(openab_core::acp::protocol::JsonRpcId::as_u64)
            == Some(request_id)
        {
            break;
        }
    }
    conn.prompt_done().await;
    assert_eq!(conn.pending_elicitations().await, 0);
    assert_eq!(presentation.form.fields.len(), 1);
}

#[test]
fn fake_agent_entry() {
    if std::env::var("OPENAB_FAKE_AGENT").as_deref() != Ok("1") {
        return;
    }
    let scenario = std::env::var("OPENAB_FAKE_SCENARIO").unwrap();
    run_fake_agent(&scenario);
    std::process::exit(0);
}

fn run_fake_agent(scenario: &str) {
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    let mut out = std::io::stdout();

    let init = read_json(&mut lines);
    assert_eq!(
        init.get("method").and_then(Value::as_str),
        Some("initialize")
    );
    let client_capabilities = init
        .pointer("/params/clientCapabilities")
        .expect("initialize must include clientCapabilities");
    match scenario {
        "no_capability" => assert_eq!(client_capabilities, &json!({})),
        _ => assert_eq!(client_capabilities, &json!({"elicitation": {"form": {}}})),
    }
    respond(
        &mut out,
        init["id"].clone(),
        json!({
            "protocolVersion": 1,
            "agentInfo": {"name": "fake-acp", "version": "test"},
            "agentCapabilities": {"loadSession": false}
        }),
    );
    if scenario == "no_capability" {
        return;
    }

    let session_new = read_json(&mut lines);
    assert_eq!(
        session_new.get("method").and_then(Value::as_str),
        Some("session/new")
    );
    respond(
        &mut out,
        session_new["id"].clone(),
        json!({"sessionId": "sess-test"}),
    );

    let prompt = read_json(&mut lines);
    assert_eq!(
        prompt.get("method").and_then(Value::as_str),
        Some("session/prompt")
    );
    let prompt_id = prompt["id"].clone();

    match scenario {
        "accept_collision" => {
            request_elicitation(&mut out, prompt_id.clone(), mixed_schema());
            notify(
                &mut out,
                json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {"sessionId":"sess-test","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"pending"}}}
                }),
            );
            let result = read_json(&mut lines);
            assert_eq!(result["id"], prompt_id);
            assert_eq!(result["result"]["action"], "accept");
            assert_eq!(result["result"]["content"]["strategy"], "balanced");
        }
        #[cfg(unix)]
        "closed_stdin" => {
            // Close only the response pipe. Keep stdout live so EOF cleanup cannot
            // mask the actual response-write failure under test.
            unsafe {
                libc::close(libc::STDIN_FILENO);
            }
            request_elicitation(&mut out, json!("elicitation-a"), small_schema());
            std::thread::sleep(std::time::Duration::from_secs(5));
            return;
        }
        "cancel_negative_id" => {
            request_elicitation(&mut out, json!(-1), small_schema());
            let result = read_json(&mut lines);
            assert_eq!(result["id"], -1);
            assert_eq!(result["result"]["action"], "cancel");
        }
        "cancel_string_id" => {
            request_elicitation(&mut out, json!("elicitation-a"), small_schema());
            let result = read_json(&mut lines);
            assert_eq!(result["id"], "elicitation-a");
            assert_eq!(result["result"]["action"], "cancel");
        }
        "eof_pending" => {
            request_elicitation(&mut out, json!(910), small_schema());
            return;
        }
        "deadline_cancel" => {
            request_elicitation(&mut out, json!(911), small_schema());
            let result = read_json(&mut lines);
            assert_eq!(result["id"], 911);
            assert_eq!(result["result"]["action"], "cancel");
            return;
        }
        "deadline_hang" => {
            request_elicitation(&mut out, json!(912), small_schema());
            std::thread::sleep(std::time::Duration::from_secs(5));
            return;
        }
        "overload" => {
            request_elicitation(&mut out, json!(900), small_schema());
            request_elicitation(&mut out, json!(901), small_schema());
            let busy = read_json(&mut lines);
            assert_eq!(busy["id"], 901);
            assert_eq!(busy["error"]["code"], -32000);
            request_elicitation(&mut out, json!(902), too_many_choices_schema());
            let invalid = read_json(&mut lines);
            assert_eq!(invalid["id"], 902);
            assert_eq!(invalid["error"]["code"], -32602);
            let first = read_json(&mut lines);
            assert_eq!(first["id"], 900);
            assert_eq!(first["result"]["action"], "decline");
        }
        other => panic!("unknown fake scenario {other}"),
    }

    notify(
        &mut out,
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sessionId":"sess-test","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"done"}}}
        }),
    );
    respond(&mut out, prompt_id, json!({"stopReason": "end_turn"}));
}

fn read_json(lines: &mut impl Iterator<Item = std::io::Result<String>>) -> Value {
    let line = lines.next().expect("expected input line").unwrap();
    serde_json::from_str(&line).unwrap()
}

fn respond(out: &mut std::io::Stdout, id: Value, result: Value) {
    writeln!(out, "{}", json!({"jsonrpc":"2.0","id":id,"result":result})).unwrap();
    out.flush().unwrap();
}

fn notify(out: &mut std::io::Stdout, value: Value) {
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}

fn request_elicitation(out: &mut std::io::Stdout, id: Value, requested_schema: Value) {
    writeln!(
        out,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "elicitation/create",
            "params": {
                "sessionId": "sess-test",
                "mode": "form",
                "message": "Choose options",
                "requestedSchema": requested_schema,
            }
        })
    )
    .unwrap();
    out.flush().unwrap();
}

fn small_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"]
    })
}

fn mixed_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "strategy": {"type":"string","enum":["conservative","balanced","aggressive"],"default":"balanced"},
            "confirm": {"type":"boolean","default":false}
        },
        "required": ["strategy", "confirm"]
    })
}

fn too_many_choices_schema() -> Value {
    let choices: Vec<String> = (0..=100).map(|i| format!("choice-{i}")).collect();
    json!({
        "type": "object",
        "properties": {"choice": {"type":"string","enum":choices}},
        "required": ["choice"]
    })
}
