use async_trait::async_trait;
use openab_core::acp::connection::AcpConnection;
use openab_core::acp::elicitation::{
    ElicitationOutcome, ElicitationPresentation, ElicitationStatus, FormPresenter,
};
use openab_core::acp::ContentBlock;
use openab_core::adapter::{ChannelRef, MessageRef};
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
            channel(),
            trigger_message(),
            HashSet::from(["user-a".to_string(), "user-b".to_string()]),
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

    let mut content = presentation.form.default_content();
    content.insert("strategy".into(), json!("balanced"));
    content.insert("confirm".into(), json!(true));
    outcome_tx
        .send(ElicitationOutcome::Accept(content))
        .unwrap();

    let mut saw_text = false;
    loop {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if msg.method.as_deref() == Some("session/update") {
            saw_text = true;
        }
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
    assert!(saw_text);
    assert_eq!(expired_rx.recv().await, Some(ElicitationStatus::Submitted));
    conn.prompt_done().await;
}

#[tokio::test]
async fn unsupported_sessions_do_not_advertise_elicitation() {
    let mut conn = spawn_fake("no_capability", None).await.unwrap();
    conn.initialize().await.unwrap();
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
            channel(),
            trigger_message(),
            HashSet::from(["user-a".to_string()]),
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
    let has_form = init
        .pointer("/params/clientCapabilities/elicitation/form")
        .is_some();
    match scenario {
        "no_capability" => assert!(!has_form),
        _ => assert!(has_form),
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
            let result = read_json(&mut lines);
            assert_eq!(result["id"], prompt_id);
            assert_eq!(result["result"]["action"], "accept");
            assert_eq!(result["result"]["content"]["strategy"], "balanced");
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
