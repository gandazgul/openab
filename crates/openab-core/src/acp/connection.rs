use crate::acp::elicitation::{
    jsonrpc_error, jsonrpc_success, presentation_failure, validate_accepted_content,
    ConnectionGeneration, ElicitationContext, ElicitationCoordinator, ElicitationOutcome,
    ElicitationStart, ElicitationStatus, ElicitationTurnContext, FormPresenter,
    MAX_ACP_FRAME_BYTES,
};
use crate::acp::protocol::{
    parse_config_options, parse_usage_report, ConfigOption, JsonRpcId, JsonRpcMessage,
    JsonRpcRequest, JsonRpcResponse, UsageReport,
};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

/// Pick the most permissive selectable permission option from ACP options.
fn pick_best_option(options: &[Value]) -> Option<String> {
    let mut fallback: Option<&Value> = None;

    for kind in ["allow_always", "allow_once"] {
        if let Some(option) = options
            .iter()
            .find(|option| option.get("kind").and_then(|k| k.as_str()) == Some(kind))
        {
            return option
                .get("optionId")
                .and_then(|id| id.as_str())
                .map(str::to_owned);
        }
    }

    for option in options {
        let kind = option.get("kind").and_then(|k| k.as_str());
        if kind == Some("reject_once") || kind == Some("reject_always") {
            continue;
        }
        fallback = Some(option);
        break;
    }

    fallback
        .and_then(|option| option.get("optionId"))
        .and_then(|id| id.as_str())
        .map(str::to_owned)
}

/// Build a spec-compliant permission response with backward-compatible fallback.
fn build_permission_response(params: Option<&Value>) -> Value {
    match params
        .and_then(|p| p.get("options"))
        .and_then(|options| options.as_array())
    {
        None => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "allow_always"
            }
        }),
        Some(options) => {
            if let Some(option_id) = pick_best_option(options) {
                json!({
                    "outcome": {
                        "outcome": "selected",
                        "optionId": option_id
                    }
                })
            } else {
                json!({
                    "outcome": {
                        "outcome": "cancelled"
                    }
                })
            }
        }
    }
}

fn expand_env(val: &str) -> String {
    if val.starts_with("${") && val.ends_with('}') {
        let key = &val[2..val.len() - 1];
        std::env::var(key).unwrap_or_default()
    } else {
        val.to_string()
    }
}
use tokio::time::Instant;

/// A content block for the ACP prompt — either text or image.
#[derive(Debug, Clone)]
pub enum ContentBlock {
    Text { text: String },
    Image { media_type: String, data: String },
}

impl ContentBlock {
    pub fn to_json(&self) -> Value {
        match self {
            ContentBlock::Text { text } => json!({
                "type": "text",
                "text": text
            }),
            ContentBlock::Image { media_type, data } => json!({
                "type": "image",
                "data": data,
                "mimeType": media_type
            }),
        }
    }
}

/// Lock-free view of session activity, readable without the connection mutex.
pub struct SessionActivity {
    /// Milliseconds since process boot (monotonic) of the last observed activity.
    last_active_ms: AtomicU64,
    /// True while a prompt turn is in flight (mutex likely held).
    prompt_in_flight: AtomicBool,
}

impl Default for SessionActivity {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionActivity {
    pub fn new() -> Self {
        Self {
            last_active_ms: AtomicU64::new(Self::now_ms()),
            prompt_in_flight: AtomicBool::new(false),
        }
    }

    /// Monotonic milliseconds since first use (process boot). SystemTime is
    /// unsuitable here: a wall-clock step (NTP, manual change) could make an
    /// active session look hours stale and trigger a false hung eviction.
    fn now_ms() -> u64 {
        use std::sync::OnceLock;
        static BOOT: OnceLock<std::time::Instant> = OnceLock::new();
        BOOT.get_or_init(std::time::Instant::now)
            .elapsed()
            .as_millis() as u64
    }

    pub fn touch(&self) {
        self.last_active_ms.store(Self::now_ms(), Ordering::Release);
    }

    pub fn set_in_flight(&self, in_flight: bool) {
        self.prompt_in_flight.store(in_flight, Ordering::Release);
    }

    /// Milliseconds since process boot of the last observed activity.
    pub fn last_active_ms(&self) -> u64 {
        self.last_active_ms.load(Ordering::Acquire)
    }

    /// Elapsed time since the last observed activity (saturating at zero).
    pub fn age(&self) -> std::time::Duration {
        let last = self.last_active_ms.load(Ordering::Acquire);
        std::time::Duration::from_millis(Self::now_ms().saturating_sub(last))
    }

    pub fn in_flight(&self) -> bool {
        self.prompt_in_flight.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_last_active_ms(&self, ms: u64) {
        self.last_active_ms.store(ms, Ordering::Release);
    }
}

pub struct AcpConnection {
    _proc: Child,
    /// PID of the direct child, used as the process group ID for cleanup.
    child_pgid: Option<i32>,
    stdin: Arc<Mutex<ChildStdin>>,
    next_id: AtomicU64,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>>,
    notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>>,
    pub acp_session_id: Option<String>,
    pub supports_load_session: bool,
    /// Agent name from `initialize` (`agentInfo.name`), e.g. "Kiro CLI Agent".
    /// Used to gate agent-specific extension methods.
    pub agent_name: String,
    pub config_options: Vec<ConfigOption>,
    pub last_active: Instant,
    pub activity: Arc<SessionActivity>,
    pub session_reset: bool,
    generation: ConnectionGeneration,
    elicitation: Arc<ElicitationCoordinator>,
    elicitation_turn: Arc<Mutex<Option<ElicitationTurnContext>>>,
    form_presented: Arc<AtomicBool>,
    form_presenter: Option<Arc<dyn FormPresenter>>,
    agent_name_shared: Arc<Mutex<String>>,
    _reader_handle: JoinHandle<()>,
    _stderr_handle: Option<JoinHandle<()>>,
    /// Revokes this session's facade token when the connection is dropped, on any evict path.
    /// Held only for its `Drop` side effect (never read).
    ///
    /// It used to cancel a per-session MCP proxy server; that server is gone and the guard now
    /// carries the minted token instead.
    #[cfg(feature = "acp-mcp")]
    #[allow(dead_code)]
    facade_token_guard: Option<tokio_util::sync::DropGuard>,
}

/// Build the final set of env vars for the agent subprocess.
/// `explicit` ([agent].env) takes precedence over `inherit` ([agent].inherit_env).
/// Returns (merged env map, list of keys that were inherited from the process).
fn build_agent_env(
    explicit: &std::collections::HashMap<String, String>,
    inherit_keys: &[String],
) -> (std::collections::HashMap<String, String>, Vec<String>) {
    let mut result: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut inherited: Vec<String> = Vec::new();

    for (k, v) in explicit {
        result.insert(k.clone(), expand_env(v));
    }

    for key in inherit_keys {
        if !result.contains_key(key) {
            if let Ok(v) = std::env::var(key) {
                result.insert(key.clone(), v);
                inherited.push(key.clone());
            }
        }
    }

    (result, inherited)
}

/// Reader loop body: reads JSON-RPC messages from `reader`, auto-replies
/// `session/request_permission` via `writer`, resolves pending responses,
/// and forwards notifications + stale id-bearing messages to the active
/// subscriber. Extracted as a free generic function so unit tests can drive
/// it with `tokio::io::duplex()` halves instead of a real child process.
pub(crate) struct ReaderLoopContext<W> {
    writer: Arc<Mutex<W>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>>,
    notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>>,
    elicitation: Arc<ElicitationCoordinator>,
    generation: ConnectionGeneration,
    elicitation_turn: Arc<Mutex<Option<ElicitationTurnContext>>>,
    form_presented: Arc<AtomicBool>,
    form_presenter: Option<Arc<dyn FormPresenter>>,
    agent_name: Arc<Mutex<String>>,
}

pub(crate) async fn run_reader_loop<R, W>(reader: R, ctx: ReaderLoopContext<W>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let ReaderLoopContext {
        writer,
        pending,
        notify_tx,
        elicitation,
        generation,
        elicitation_turn,
        form_presented,
        form_presenter,
        agent_name,
    } = ctx;
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        line.clear();
        let mut close_reader = false;
        loop {
            match reader.fill_buf().await {
                Ok([]) => break,
                Ok(chunk) => {
                    let newline = chunk.iter().position(|byte| *byte == b'\n');
                    let consumed = newline.map_or(chunk.len(), |index| index + 1);
                    if consumed > MAX_ACP_FRAME_BYTES - line.len() {
                        error!(
                            limit = MAX_ACP_FRAME_BYTES,
                            "ACP frame exceeded maximum size"
                        );
                        // This stdout pipe belongs to one child and is never reused.
                        // Drop it without draining: an untrusted child could otherwise
                        // keep cleanup blocked forever by sending an endless frame.
                        close_reader = true;
                        break;
                    }
                    line.extend_from_slice(&chunk[..consumed]);
                    reader.consume(consumed);
                    if newline.is_some() {
                        break;
                    }
                }
                Err(e) => {
                    error!("reader error: {e}");
                    close_reader = true;
                    break;
                }
            }
        }
        if close_reader || line.is_empty() {
            break;
        }
        let raw = match std::str::from_utf8(&line) {
            Ok(raw) => raw.trim_end_matches(['\r', '\n']),
            Err(_) => continue,
        };
        let msg: JsonRpcMessage = match serde_json::from_str(raw) {
            Ok(m) => m,
            Err(_) => continue,
        };
        debug!(method = ?msg.method, has_id = msg.id.is_some(), "acp_recv");

        // Agent-to-client requests must be classified before outbound response matching.
        if let (Some(method), Some(agent_request_id)) = (msg.method.as_deref(), msg.id.clone()) {
            if method == "session/request_permission" {
                let title = msg
                    .params
                    .as_ref()
                    .and_then(|p| p.get("toolCall"))
                    .and_then(|t| t.get("title"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("?");

                let outcome = build_permission_response(msg.params.as_ref());
                info!(title, %outcome, "auto-respond permission");
                let reply = JsonRpcResponse::new(agent_request_id, outcome);
                if let Ok(data) = serde_json::to_string(&reply) {
                    let _ = write_json_line(writer.clone(), data).await;
                }
                continue;
            }

            if method == "elicitation/create" {
                let Some(params) = msg.params.as_ref() else {
                    let err = crate::acp::elicitation::ElicitationError::invalid_params(
                        "elicitation/create params are required",
                    );
                    if let Ok(data) = serde_json::to_string(&jsonrpc_error(agent_request_id, err)) {
                        let _ = write_json_line(writer.clone(), data).await;
                    }
                    continue;
                };
                let Some(presenter) = form_presenter.clone() else {
                    let err = crate::acp::elicitation::ElicitationError::invalid_params(
                        "form elicitation is not supported for this session",
                    );
                    if let Ok(data) = serde_json::to_string(&jsonrpc_error(agent_request_id, err)) {
                        let _ = write_json_line(writer.clone(), data).await;
                    }
                    continue;
                };
                let turn = elicitation_turn.lock().await.clone();
                let display_agent_name = agent_name.lock().await.clone();
                match elicitation
                    .start(
                        generation,
                        agent_request_id.clone(),
                        params,
                        raw.len(),
                        turn.as_ref(),
                        &display_agent_name,
                    )
                    .await
                {
                    ElicitationStart::Immediate(outcome) => {
                        if let Ok(data) = serde_json::to_string(&jsonrpc_success(
                            agent_request_id,
                            outcome.to_result_value(),
                        )) {
                            let _ = write_json_line(writer.clone(), data).await;
                        }
                    }
                    ElicitationStart::Error(err) => {
                        if let Ok(data) =
                            serde_json::to_string(&jsonrpc_error(agent_request_id, err))
                        {
                            let _ = write_json_line(writer.clone(), data).await;
                        }
                    }
                    ElicitationStart::Present(lease) => {
                        form_presented.store(true, Ordering::Release);
                        let writer = writer.clone();
                        let elicitation = elicitation.clone();
                        tokio::spawn(async move {
                            let (presentation, mut receiver) = lease.into_parts();
                            let id = presentation.agent_request_id.clone();
                            let generation = presentation.generation;
                            let nonce = presentation.nonce.clone();
                            let form = presentation.form.clone();
                            let presenter_task = presenter.clone();
                            let present_task = tokio::spawn(async move {
                                presenter_task.present_form(presentation).await
                            });
                            let outcome = tokio::select! {
                                outcome = async { (&mut receiver).await.unwrap_or(ElicitationOutcome::Cancel) } => {
                                    validate_accepted_content(&form, outcome)
                                }
                                result = present_task => {
                                    let result = result
                                        .map_err(|e| presentation_failure(anyhow::anyhow!("elicitation presentation task failed: {e}")))
                                        .and_then(|result| result.map_err(presentation_failure));
                                    match result
                                        .and_then(|outcome| validate_accepted_content(&form, outcome))
                                    {
                                        Ok(outcome) => {
                                            if elicitation.complete_generation(generation, &nonce).await {
                                                Ok(outcome)
                                            } else {
                                                validate_accepted_content(
                                                    &form,
                                                    receiver.await.unwrap_or(ElicitationOutcome::Cancel),
                                                )
                                            }
                                        }
                                        Err(err) => {
                                            if elicitation.complete_generation(generation, &nonce).await {
                                                Err(err)
                                            } else {
                                                validate_accepted_content(
                                                    &form,
                                                    receiver.await.unwrap_or(ElicitationOutcome::Cancel),
                                                )
                                            }
                                        }
                                    }
                                }
                            };
                            let mut status = match &outcome {
                                Ok(ElicitationOutcome::Accept(_)) => ElicitationStatus::Submitted,
                                Ok(ElicitationOutcome::Decline) => ElicitationStatus::Declined,
                                Ok(ElicitationOutcome::Cancel) => ElicitationStatus::Cancelled,
                                Err(_) => ElicitationStatus::Expired,
                            };
                            let envelope = match outcome {
                                Ok(outcome) => jsonrpc_success(id, outcome.to_result_value()),
                                Err(err) => jsonrpc_error(id, err),
                            };
                            // The next question may arrive as soon as the Agent reads this reply.
                            // Release admission before publishing it; Discord message cleanup is cosmetic.
                            elicitation.finish_generation(generation, &nonce).await;
                            let delivered = if let Ok(data) = serde_json::to_string(&envelope) {
                                write_json_line(writer, data).await.is_ok()
                            } else {
                                false
                            };
                            if !delivered {
                                status = ElicitationStatus::Expired;
                                warn!("failed to write elicitation response");
                            }
                            let _ = presenter.expire_form(&nonce, status).await;
                        });
                    }
                }
                continue;
            }

            let err = crate::acp::protocol::JsonRpcError {
                code: -32601,
                message: format!("unsupported agent request method `{method}`"),
                data: None,
            };
            if let Ok(data) = serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "id": agent_request_id,
                "error": {"code": err.code, "message": err.message},
            })) {
                let _ = write_json_line(writer.clone(), data).await;
            }
            continue;
        }

        // Response (numeric id) → resolve pending AND forward to subscriber.
        if let Some(id) = msg.id.as_ref().and_then(JsonRpcId::as_u64) {
            let mut map = pending.lock().await;
            if let Some(tx) = map.remove(&id) {
                // Forward to subscriber so they see the completion
                let sub = notify_tx.lock().await;
                if let Some(ntx) = sub.as_ref() {
                    // Clone the essential fields for the subscriber
                    let _ = ntx.send(JsonRpcMessage {
                        id: Some(JsonRpcId::Number(id as i64)),
                        method: None,
                        result: msg.result.clone(),
                        error: msg.error.clone(),
                        params: None,
                    });
                }
                let _ = tx.send(msg);
                continue;
            }
            // Stale id (#732): pending was already abandoned. Falls through
            // to subscriber forwarding; the adapter recv loop filters by
            // request_id so it can't leak into the next prompt.
            trace!(request_id = id, "stale id-bearing message after abandon");
        }

        // Notification → forward to subscriber
        let sub = notify_tx.lock().await;
        if let Some(tx) = sub.as_ref() {
            let _ = tx.send(msg);
        }
    }

    elicitation
        .expire_generation(generation, ElicitationOutcome::Cancel)
        .await;

    // Connection closed — resolve all pending with error
    let mut map = pending.lock().await;
    for (_, tx) in map.drain() {
        let _ = tx.send(JsonRpcMessage {
            id: None,
            method: None,
            result: None,
            error: Some(crate::acp::protocol::JsonRpcError {
                code: -1,
                message: "connection closed".into(),
                data: None,
            }),
            params: None,
        });
    }
    // Close the notify channel so rx.recv() returns None
    let mut sub = notify_tx.lock().await;
    *sub = None;
}

async fn write_json_line<W>(writer: Arc<Mutex<W>>, data: String) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut w = writer.lock().await;
    w.write_all(data.as_bytes()).await?;
    w.write_all(b"\n").await?;
    w.flush().await
}

impl AcpConnection {
    pub async fn spawn(
        command: &str,
        args: &[String],
        working_dir: &str,
        env: &std::collections::HashMap<String, String>,
        inherit_env: &[String],
    ) -> Result<Self> {
        Self::spawn_with_elicitation(command, args, working_dir, env, inherit_env, None).await
    }

    pub async fn spawn_with_elicitation(
        command: &str,
        args: &[String],
        working_dir: &str,
        env: &std::collections::HashMap<String, String>,
        inherit_env: &[String],
        form_presenter: Option<Arc<dyn FormPresenter>>,
    ) -> Result<Self> {
        info!(cmd = command, ?args, cwd = working_dir, "spawning agent");

        let mut cmd = tokio::process::Command::new(command);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .current_dir(working_dir);
        // Create a new process group so we can kill the entire tree.
        // SAFETY: setpgid is async-signal-safe (POSIX.1-2008) and called
        // before exec. Return value checked — failure means the child won't
        // have its own process group, so kill(-pgid) would be unsafe.
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        #[cfg(windows)]
        {
            cmd.creation_flags(0x00000200); // CREATE_NEW_PROCESS_GROUP
        }
        // Clear inherited env to prevent credential leakage (e.g. DISCORD_BOT_TOKEN).
        // Only [agent].env values + essential baseline vars are passed through.
        cmd.env_clear();
        // Preserve the real HOME so agents can find OAuth/auth files (~/.codex,
        // ~/.claude, ~/.config/gh, etc.). working_dir is already set via
        // current_dir() above and is not necessarily the user's home directory.
        cmd.env(
            "HOME",
            std::env::var("HOME").unwrap_or_else(|_| working_dir.into()),
        );
        cmd.env(
            "PATH",
            std::env::var("PATH").unwrap_or_else(|_| "/usr/local/bin:/usr/bin:/bin".into()),
        );
        #[cfg(unix)]
        {
            cmd.env(
                "USER",
                std::env::var("USER").unwrap_or_else(|_| "agent".into()),
            );
        }
        #[cfg(windows)]
        {
            // Windows requires SystemRoot for DLL loading and basic OS functionality.
            // USERPROFILE is the Windows equivalent of HOME.
            cmd.env(
                "USERPROFILE",
                std::env::var("USERPROFILE").unwrap_or_else(|_| working_dir.into()),
            );
            cmd.env(
                "USERNAME",
                std::env::var("USERNAME").unwrap_or_else(|_| "agent".into()),
            );
            if let Ok(v) = std::env::var("SystemRoot") {
                cmd.env("SystemRoot", v);
            }
            if let Ok(v) = std::env::var("SystemDrive") {
                cmd.env("SystemDrive", v);
            }
        }
        for (k, v) in env {
            cmd.env(k, expand_env(v));
        }
        // Inherit selected env vars from the OAB process (e.g. vars injected
        // via Kubernetes envFrom).  Keys already in [agent].env are skipped —
        // explicit values take precedence.
        let (agent_env, inherited_keys) = build_agent_env(env, inherit_env);
        for (k, v) in &agent_env {
            cmd.env(k, v);
        }
        if !agent_env.is_empty() {
            let explicit_keys: Vec<&String> = env.keys().collect();
            tracing::warn!(
                ?explicit_keys,
                ?inherited_keys,
                "[agent].env/inherit_env is set -- these values are accessible to the agent and could be exfiltrated via prompt injection"
            );
        }
        let mut proc = cmd
            .spawn()
            .map_err(|e| anyhow!("failed to spawn {command}: {e}"))?;
        let child_pgid = proc.id().and_then(|pid| i32::try_from(pid).ok());

        let stdout = proc.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
        let stdin = proc.stdin.take().ok_or_else(|| anyhow!("no stdin"))?;
        let stdin = Arc::new(Mutex::new(stdin));

        // Capture agent stderr and log it (ACP spec: agents MAY write to stderr
        // for logging; clients MAY capture or ignore it).
        let stderr_handle = if let Some(stderr) = proc.stderr.take() {
            let cmd_name = command.to_string();
            Some(tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim();
                            if !trimmed.is_empty() {
                                let sanitized: String = trimmed
                                    .chars()
                                    .filter(|c| !c.is_control() || *c == '\t')
                                    .collect();
                                if !sanitized.is_empty() {
                                    tracing::warn!(agent = %cmd_name, "{sanitized}");
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }))
        } else {
            None
        };

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));
        let elicitation = ElicitationCoordinator::new();
        let generation = ConnectionGeneration::new();
        let elicitation_turn = Arc::new(Mutex::new(None));
        let form_presented = Arc::new(AtomicBool::new(false));
        let agent_name_shared = Arc::new(Mutex::new(command.to_string()));

        let reader_handle = tokio::spawn(run_reader_loop(
            stdout,
            ReaderLoopContext {
                writer: stdin.clone(),
                pending: pending.clone(),
                notify_tx: notify_tx.clone(),
                elicitation: elicitation.clone(),
                generation,
                elicitation_turn: elicitation_turn.clone(),
                form_presented: form_presented.clone(),
                form_presenter: form_presenter.clone(),
                agent_name: agent_name_shared.clone(),
            },
        ));

        let activity = Arc::new(SessionActivity::new());

        Ok(Self {
            _proc: proc,
            child_pgid,
            stdin,
            next_id: AtomicU64::new(1),
            pending,
            notify_tx,
            acp_session_id: None,
            supports_load_session: false,
            agent_name: command.to_string(),
            config_options: Vec::new(),
            last_active: Instant::now(),
            activity,
            session_reset: false,
            generation,
            elicitation,
            elicitation_turn,
            form_presented,
            form_presenter,
            agent_name_shared,
            _reader_handle: reader_handle,
            _stderr_handle: stderr_handle,
            #[cfg(feature = "acp-mcp")]
            facade_token_guard: None,
        })
    }

    /// Attach the guard that revokes this session's facade token when the connection drops.
    #[cfg(feature = "acp-mcp")]
    pub fn set_facade_token_guard(&mut self, guard: Option<tokio_util::sync::DropGuard>) {
        self.facade_token_guard = guard;
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) async fn send_raw(&self, data: &str) -> Result<()> {
        debug!(data = data.trim(), "acp_send");
        // A hung agent can stop draining stdin; bound the write so callers
        // (and the mutexes they hold) can never block on it indefinitely.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let mut w = self.stdin.lock().await;
            w.write_all(data.as_bytes()).await?;
            w.write_all(b"\n").await?;
            w.flush().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow!("stdin write timeout"))??;
        Ok(())
    }

    async fn send_request(&self, method: &str, params: Option<Value>) -> Result<JsonRpcMessage> {
        let id = self.next_id();
        let req = JsonRpcRequest::new(id, method, params);
        let data = serde_json::to_string(&req)?;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        self.send_raw(&data).await?;

        let timeout_secs = if method == "session/new" { 120 } else { 30 };
        let resp = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), rx)
            .await
            .map_err(|_| anyhow!("timeout waiting for {method} response"))?
            .map_err(|_| anyhow!("channel closed waiting for {method}"))?;

        if let Some(err) = &resp.error {
            return Err(anyhow!("{err}"));
        }
        Ok(resp)
    }

    pub async fn initialize(&mut self) -> Result<()> {
        let client_capabilities = if self.form_presenter.is_some() {
            json!({"elicitation": {"form": {}}})
        } else {
            json!({})
        };
        let resp = self
            .send_request(
                "initialize",
                Some(json!({
                    "protocolVersion": 1,
                    "clientCapabilities": client_capabilities,
                    "clientInfo": {"name": "openab", "version": "0.1.0"},
                })),
            )
            .await?;

        let result = resp.result.as_ref();
        let agent_name = result
            .and_then(|r| r.get("agentInfo"))
            .and_then(|a| a.get("name"))
            .and_then(|n| n.as_str())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.agent_name.clone());
        self.agent_name = agent_name.clone();
        *self.agent_name_shared.lock().await = self.agent_name.clone();
        self.supports_load_session = result
            .and_then(|r| r.get("agentCapabilities"))
            .and_then(|c| c.get("loadSession"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        info!(
            agent = %agent_name,
            load_session = self.supports_load_session,
            "initialized"
        );
        Ok(())
    }

    pub async fn session_new(&mut self, cwd: &str) -> Result<String> {
        let resp = self
            .send_request("session/new", Some(json!({"cwd": cwd, "mcpServers": []})))
            .await?;

        let session_id = resp
            .result
            .as_ref()
            .and_then(|r| r.get("sessionId"))
            .and_then(|s| s.as_str())
            .ok_or_else(|| anyhow!("no sessionId in session/new response"))?
            .to_string();

        info!(session_id = %session_id, "session created");
        self.acp_session_id = Some(session_id.clone());
        if let Some(result) = resp.result.as_ref() {
            self.config_options = parse_config_options(result);
            if !self.config_options.is_empty() {
                info!(count = self.config_options.len(), "parsed configOptions");
            }
        }
        Ok(session_id)
    }

    /// Set a config option (e.g. model, mode) via ACP session/set_config_option.
    /// Returns the updated list of all config options.
    pub async fn set_config_option(
        &mut self,
        config_id: &str,
        value: &str,
    ) -> Result<Vec<ConfigOption>> {
        let session_id = self
            .acp_session_id
            .as_ref()
            .ok_or_else(|| anyhow!("no session"))?
            .clone();

        let resp = self
            .send_request(
                "session/set_config_option",
                Some(json!({
                    "sessionId": session_id,
                    "configId": config_id,
                    "value": value,
                })),
            )
            .await;

        match resp {
            Ok(r) => {
                if let Some(result) = r.result.as_ref() {
                    self.config_options = parse_config_options(result);
                }
                info!(config_id, value, "config option set");
            }
            Err(_) => {
                // Fall back: send as a slash command (e.g. "/model claude-sonnet-4")
                let cmd = format!("/{config_id} {value}");
                info!(
                    cmd,
                    "set_config_option not supported, falling back to prompt"
                );
                let _resp = self
                    .send_request(
                        "session/prompt",
                        Some(json!({
                            "sessionId": session_id,
                            "prompt": [{"type": "text", "text": cmd}],
                        })),
                    )
                    .await?;
                for opt in &mut self.config_options {
                    if opt.id == config_id {
                        opt.current_value = value.to_string();
                    }
                }
            }
        }

        Ok(self.config_options.clone())
    }

    /// Query account-level usage/billing via kiro-cli's
    /// `_kiro.dev/commands/execute` extension (the `/usage` slash command).
    ///
    /// This is a Kiro-specific extension, not part of the ACP spec, and the
    /// request shape is strict: a malformed `command` value is a
    /// deserialization error that kills the whole ACP connection (no JSON-RPC
    /// error is returned). We therefore gate on the agent name advertised in
    /// `initialize` and never retry on failure.
    pub async fn get_usage(&mut self) -> Result<UsageReport> {
        if !self.agent_name.to_ascii_lowercase().contains("kiro") {
            return Err(anyhow!(
                "usage query is not supported by this backend ({})",
                if self.agent_name.is_empty() {
                    "unknown agent"
                } else {
                    &self.agent_name
                }
            ));
        }
        let session_id = self
            .acp_session_id
            .as_ref()
            .ok_or_else(|| anyhow!("no session"))?
            .clone();

        let resp = self
            .send_request(
                "_kiro.dev/commands/execute",
                Some(json!({
                    "sessionId": session_id,
                    // Adjacently-tagged TuiCommand enum: tag = "command", content = "args".
                    "command": {"command": "usage", "args": {}},
                })),
            )
            .await?;

        let result = resp
            .result
            .as_ref()
            .ok_or_else(|| anyhow!("empty usage response"))?;
        parse_usage_report(result)
            .ok_or_else(|| anyhow!("could not parse usage response from agent"))
    }

    /// Send a prompt with content blocks (text and/or images) and return a receiver
    /// for streaming notifications. The final message on the channel will have id set
    /// (the prompt response).
    pub async fn set_elicitation_turn(&self, context: Option<ElicitationTurnContext>) {
        *self.elicitation_turn.lock().await = context;
    }

    pub fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub async fn pending_elicitations(&self) -> usize {
        self.elicitation.active_count().await
    }

    pub fn elicitation_handle(&self) -> (Arc<ElicitationCoordinator>, ConnectionGeneration) {
        (self.elicitation.clone(), self.generation)
    }

    /// Start a turn. `None` supplies no human elicitation authority.
    /// Request and generation authority always come from this connection.
    pub async fn session_prompt(
        &mut self,
        content_blocks: Vec<ContentBlock>,
        elicitation_context: Option<ElicitationContext>,
    ) -> Result<(mpsc::UnboundedReceiver<JsonRpcMessage>, u64)> {
        self.last_active = Instant::now();
        self.activity.touch();
        self.activity.set_in_flight(true);
        self.form_presented.store(false, Ordering::Release);

        let session_id = self
            .acp_session_id
            .as_ref()
            .ok_or_else(|| anyhow!("no session"))?;

        let (tx, rx) = mpsc::unbounded_channel();
        *self.notify_tx.lock().await = Some(tx);

        let id = self.next_id();
        let authority_id = self.elicitation.open_generation_turn(self.generation);
        *self.elicitation_turn.lock().await =
            elicitation_context.map(|context| ElicitationTurnContext {
                authority_id,
                session_id: self.acp_session_id.clone(),
                request_id: Some(id),
                channel: context.channel,
                trigger_message: context.trigger_message,
                authorized_user_ids: context.authorized_user_ids,
            });

        // Convert content blocks to JSON
        let prompt_json: Vec<Value> = content_blocks.iter().map(|b| b.to_json()).collect();

        let req = JsonRpcRequest::new(
            id,
            "session/prompt",
            Some(json!({
                "sessionId": session_id,
                "prompt": prompt_json,
            })),
        );
        let data = serde_json::to_string(&req)?;

        let (resp_tx, _resp_rx) = oneshot::channel();
        self.pending.lock().await.insert(id, resp_tx);

        if let Err(err) = self.send_raw(&data).await {
            self.pending.lock().await.remove(&id);
            self.elicitation
                .expire_generation(self.generation, ElicitationOutcome::Cancel)
                .await;
            *self.elicitation_turn.lock().await = None;
            *self.notify_tx.lock().await = None;
            self.activity.set_in_flight(false);
            return Err(err);
        }
        Ok((rx, id))
    }

    /// Whether the current turn admitted a form, creating a separate chat message.
    pub(crate) fn form_presented(&self) -> bool {
        self.form_presented.load(Ordering::Acquire)
    }

    /// Call after prompt streaming is done to clean up subscriber.
    pub async fn prompt_done(&mut self) {
        self.elicitation
            .expire_generation(self.generation, ElicitationOutcome::Cancel)
            .await;
        *self.elicitation_turn.lock().await = None;
        *self.notify_tx.lock().await = None;
        self.activity.touch();
        self.activity.set_in_flight(false);
        self.last_active = Instant::now();
    }

    /// Drop the pending entry for `request_id` and best-effort send
    /// `session/cancel` as a JSON-RPC notification (no id; per ACP spec the
    /// agent does not reply). Errors are swallowed: the agent process may
    /// already be dead, in which case the stdin write fails harmlessly.
    /// See #732.
    pub async fn abandon_request(&self, request_id: u64) {
        self.elicitation
            .expire_generation(self.generation, ElicitationOutcome::Cancel)
            .await;
        self.pending.lock().await.remove(&request_id);
        *self.elicitation_turn.lock().await = None;
        *self.notify_tx.lock().await = None;
        self.activity.set_in_flight(false);
        let Some(session_id) = self.acp_session_id.as_deref() else {
            return;
        };
        let req = json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id},
        });
        if let Ok(data) = serde_json::to_string(&req) {
            let _ = self.send_raw(&data).await;
        }
    }

    /// Return a clone of the stdin handle for lock-free cancel.
    pub fn cancel_handle(&self) -> Arc<Mutex<ChildStdin>> {
        Arc::clone(&self.stdin)
    }

    pub fn activity_handle(&self) -> Arc<SessionActivity> {
        Arc::clone(&self.activity)
    }

    /// Process-group id of the agent child, readable without any lock state.
    pub fn child_pgid(&self) -> Option<i32> {
        self.child_pgid
    }

    pub fn alive(&self) -> bool {
        !self._reader_handle.is_finished()
    }

    /// Resume a previous session by ID. Returns Ok(()) if the agent accepted
    /// the load, or an error if it failed (caller should fall back to session/new).
    pub async fn session_load(&mut self, session_id: &str, cwd: &str) -> Result<()> {
        let resp = self
            .send_request(
                "session/load",
                Some(json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []})),
            )
            .await?;
        // Accept any non-error response as success
        if resp.error.is_some() {
            return Err(anyhow!("session/load rejected"));
        }
        info!(session_id, "session loaded");
        self.acp_session_id = Some(session_id.to_string());
        if let Some(result) = resp.result.as_ref() {
            self.config_options = parse_config_options(result);
        }
        Ok(())
    }

    /// Kill the entire process group: SIGTERM → SIGKILL.
    /// Uses std::thread (not tokio::spawn) so SIGKILL fires even during
    /// runtime shutdown or panic unwinding.
    fn kill_process_group(&mut self) {
        let pgid = match self.child_pgid {
            Some(pid) if pid > 0 => pid,
            _ => return,
        };
        #[cfg(unix)]
        {
            // Stage 1: SIGTERM the process group
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
            }
            // Stage 2: SIGKILL after brief grace (std::thread survives runtime shutdown)
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(1500));
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
            });
        }
        #[cfg(not(unix))]
        {
            let _ = pgid; // suppress unused warning on Windows
        }
    }
}

impl Drop for AcpConnection {
    fn drop(&mut self) {
        if let Some(handle) = self._stderr_handle.take() {
            handle.abort();
        }
        let coordinator = self.elicitation.clone();
        let generation = self.generation;
        // Revocation is synchronous: it blocks new starts and stale UI actions
        // before Drop returns. Async expiry only resolves the old lease and
        // updates its UI; neither needs a live child. Thus process kill and UI
        // cleanup may run in either order without granting successor authority.
        coordinator.revoke_generation(generation);
        tokio::spawn(async move {
            coordinator
                .expire_generation(generation, ElicitationOutcome::Cancel)
                .await;
        });
        self.kill_process_group();
    }
}

#[cfg(test)]
mod tests {
    use super::{build_agent_env, build_permission_response, pick_best_option};
    use serde_json::json;

    #[test]
    fn picks_allow_always_over_other_options() {
        let options = vec![
            json!({"kind": "allow_once", "optionId": "once"}),
            json!({"kind": "allow_always", "optionId": "always"}),
            json!({"kind": "reject_once", "optionId": "reject"}),
        ];

        assert_eq!(pick_best_option(&options), Some("always".to_string()));
    }

    #[test]
    fn falls_back_to_first_unknown_non_reject_kind() {
        let options = vec![
            json!({"kind": "reject_once", "optionId": "reject"}),
            json!({"kind": "workspace_write", "optionId": "workspace-write"}),
        ];

        assert_eq!(
            pick_best_option(&options),
            Some("workspace-write".to_string())
        );
    }

    #[test]
    fn selects_bypass_permissions_for_exit_plan_mode() {
        let options = vec![
            json!({"optionId": "bypassPermissions", "kind": "allow_always"}),
            json!({"optionId": "acceptEdits", "kind": "allow_always"}),
            json!({"optionId": "default", "kind": "allow_once"}),
            json!({"optionId": "plan", "kind": "reject_once"}),
        ];

        assert_eq!(
            pick_best_option(&options),
            Some("bypassPermissions".to_string())
        );
    }

    #[test]
    fn returns_none_when_only_reject_options_exist() {
        let options = vec![
            json!({"kind": "reject_once", "optionId": "reject-once"}),
            json!({"kind": "reject_always", "optionId": "reject-always"}),
        ];

        assert_eq!(pick_best_option(&options), None);
    }

    #[test]
    fn builds_cancelled_outcome_when_no_selectable_option_exists() {
        let response = build_permission_response(Some(&json!({
            "options": [
                {"kind": "reject_once", "optionId": "reject-once"}
            ]
        })));

        assert_eq!(response, json!({"outcome": {"outcome": "cancelled"}}));
    }

    #[test]
    fn builds_cancelled_when_options_array_is_empty() {
        let response = build_permission_response(Some(&json!({
            "options": []
        })));

        assert_eq!(response, json!({"outcome": {"outcome": "cancelled"}}));
    }

    #[test]
    fn falls_back_to_allow_always_when_options_are_missing() {
        let response = build_permission_response(Some(&json!({
            "toolCall": {"title": "legacy"}
        })));

        assert_eq!(
            response,
            json!({"outcome": {"outcome": "selected", "optionId": "allow_always"}})
        );
    }

    #[test]
    fn falls_back_to_allow_always_when_params_is_none() {
        let response = build_permission_response(None);

        assert_eq!(
            response,
            json!({"outcome": {"outcome": "selected", "optionId": "allow_always"}})
        );
    }

    #[test]
    fn explicit_env_takes_precedence_over_inherit_env() {
        let key = "OAB_TEST_PRECEDENCE";
        std::env::set_var(key, "from_process");
        let mut explicit = std::collections::HashMap::new();
        explicit.insert(key.to_string(), "from_config".to_string());
        let inherit = vec![key.to_string()];

        let (result, inherited) = build_agent_env(&explicit, &inherit);

        assert_eq!(result.get(key).unwrap(), "from_config");
        assert!(!inherited.contains(&key.to_string()));
        std::env::remove_var(key);
    }

    #[test]
    fn inherit_env_copies_from_process() {
        let key = "OAB_TEST_INHERIT";
        std::env::set_var(key, "process_value");
        let explicit = std::collections::HashMap::new();
        let inherit = vec![key.to_string()];

        let (result, inherited) = build_agent_env(&explicit, &inherit);

        assert_eq!(result.get(key).unwrap(), "process_value");
        assert!(inherited.contains(&key.to_string()));
        std::env::remove_var(key);
    }

    #[test]
    fn inherit_env_skips_missing_vars() {
        let explicit = std::collections::HashMap::new();
        let inherit = vec!["OAB_TEST_NONEXISTENT_VAR_12345".to_string()];

        let (result, inherited) = build_agent_env(&explicit, &inherit);

        assert!(!result.contains_key("OAB_TEST_NONEXISTENT_VAR_12345"));
        assert!(inherited.is_empty());
    }
}

#[cfg(test)]
mod reader_loop_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::{duplex, AsyncWriteExt};
    use tokio::sync::{mpsc, oneshot, Mutex};

    fn test_reader_context<W>(
        writer: Arc<Mutex<W>>,
        pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>>,
        notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>>,
    ) -> ReaderLoopContext<W> {
        ReaderLoopContext {
            writer,
            pending,
            notify_tx,
            elicitation: ElicitationCoordinator::new(),
            generation: ConnectionGeneration::new(),
            elicitation_turn: Arc::new(Mutex::new(None)),
            form_presented: Arc::new(AtomicBool::new(false)),
            form_presenter: None,
            agent_name: Arc::new(Mutex::new(String::new())),
        }
    }

    /// #732 stale-id path: when a response arrives for an id the broker has
    /// already abandoned, the reader must (a) not crash, (b) leave `pending`
    /// untouched, and (c) still forward the message to whoever is currently
    /// subscribed — the adapter recv loop is responsible for filtering by
    /// request_id so the stray response never leaks into the next prompt.
    #[tokio::test]
    async fn stale_id_response_is_forwarded_without_pending_entry() {
        let (mut agent_stdout_writer, agent_stdout_reader) = duplex(8 * 1024);
        let (agent_stdin_writer, _agent_stdin_reader) = duplex(8 * 1024);

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));

        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        *notify_tx.lock().await = Some(sub_tx);

        let writer = Arc::new(Mutex::new(agent_stdin_writer));
        let handle = tokio::spawn(run_reader_loop(
            agent_stdout_reader,
            test_reader_context(writer, pending.clone(), notify_tx.clone()),
        ));

        let stale = b"{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{\"stopReason\":\"ok\"}}\n";
        agent_stdout_writer.write_all(stale).await.unwrap();
        agent_stdout_writer.flush().await.unwrap();

        let forwarded = tokio::time::timeout(std::time::Duration::from_secs(2), sub_rx.recv())
            .await
            .expect("subscriber should receive stale message before timeout")
            .expect("subscriber channel should not be closed");
        assert_eq!(forwarded.id, Some(JsonRpcId::Number(42)));
        assert!(pending.lock().await.is_empty());

        drop(agent_stdout_writer);
        handle.await.unwrap();
    }

    /// Matched-id path: when a response's id is in `pending`, the loop must
    /// resolve the oneshot AND forward a copy to the subscriber so the
    /// adapter's recv loop sees the completion. Guards against regressions
    /// that would suppress the forward branch while keeping resolve.
    #[tokio::test]
    async fn matched_id_response_resolves_pending_and_forwards() {
        let (mut agent_stdout_writer, agent_stdout_reader) = duplex(8 * 1024);
        let (agent_stdin_writer, _agent_stdin_reader) = duplex(8 * 1024);

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));

        let (resp_tx, resp_rx) = oneshot::channel();
        pending.lock().await.insert(7, resp_tx);

        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        *notify_tx.lock().await = Some(sub_tx);

        let writer = Arc::new(Mutex::new(agent_stdin_writer));
        let handle = tokio::spawn(run_reader_loop(
            agent_stdout_reader,
            test_reader_context(writer, pending.clone(), notify_tx.clone()),
        ));

        let payload = b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"stopReason\":\"end_turn\"}}\n";
        agent_stdout_writer.write_all(payload).await.unwrap();
        agent_stdout_writer.flush().await.unwrap();

        let resolved = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx)
            .await
            .expect("oneshot should resolve")
            .expect("oneshot should not be cancelled");
        assert_eq!(resolved.id, Some(JsonRpcId::Number(7)));

        let forwarded = tokio::time::timeout(std::time::Duration::from_secs(2), sub_rx.recv())
            .await
            .expect("subscriber should receive forwarded copy")
            .expect("subscriber channel should not be closed");
        assert_eq!(forwarded.id, Some(JsonRpcId::Number(7)));
        assert!(pending.lock().await.is_empty());

        drop(agent_stdout_writer);
        handle.await.unwrap();
    }

    async fn assert_frame_boundary(size: usize, newline: bool, accepted: bool) {
        let (mut output, input) = duplex(MAX_ACP_FRAME_BYTES + 32);
        let (writer, _response_reader) = duplex(1024);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let notify_tx = Arc::new(Mutex::new(Some(tx)));
        let handle = tokio::spawn(run_reader_loop(
            input,
            test_reader_context(Arc::new(Mutex::new(writer)), pending, notify_tx),
        ));
        let mut frame = br#"{"jsonrpc":"2.0","method":"boundary"}"#.to_vec();
        frame.resize(size - usize::from(newline), b' ');
        if newline {
            frame.push(b'\n');
        }
        output.write_all(&frame).await.unwrap();
        drop(output);
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap();
        assert_eq!(message.is_some(), accepted);
        if let Some(message) = message {
            assert_eq!(message.method.as_deref(), Some("boundary"));
        }
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn reader_accepts_frames_at_limit_with_newline_or_eof() {
        for size in [
            8191,
            8192,
            8193,
            MAX_ACP_FRAME_BYTES - 1,
            MAX_ACP_FRAME_BYTES,
        ] {
            assert_frame_boundary(size, true, true).await;
            assert_frame_boundary(size, false, true).await;
        }
    }

    #[tokio::test]
    async fn reader_rejects_over_limit_with_newline_or_eof() {
        assert_frame_boundary(MAX_ACP_FRAME_BYTES + 1, true, false).await;
        assert_frame_boundary(MAX_ACP_FRAME_BYTES + 1, false, false).await;
    }

    #[tokio::test]
    async fn reader_preserves_multiple_frames_in_one_chunk_and_final_eof_frame() {
        let input = b"{\"method\":\"first\"}\n{\"method\":\"second\"}\r\n{\"method\":\"last\"}";
        let (writer, _response_reader) = duplex(1024);
        let (tx, mut rx) = mpsc::unbounded_channel();
        run_reader_loop(
            &input[..],
            test_reader_context(
                Arc::new(Mutex::new(writer)),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(Some(tx))),
            ),
        )
        .await;
        for expected in ["first", "second", "last"] {
            assert_eq!(rx.recv().await.unwrap().method.as_deref(), Some(expected));
        }
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn over_limit_frame_closes_reader_and_resolves_pending() {
        let (mut agent_stdout_writer, agent_stdout_reader) = duplex(MAX_ACP_FRAME_BYTES + 32);
        let (agent_stdin_writer, _agent_stdin_reader) = duplex(8 * 1024);

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (resp_tx, resp_rx) = oneshot::channel();
        pending.lock().await.insert(7, resp_tx);
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));

        let handle = tokio::spawn(run_reader_loop(
            agent_stdout_reader,
            test_reader_context(
                Arc::new(Mutex::new(agent_stdin_writer)),
                pending.clone(),
                notify_tx,
            ),
        ));

        let payload = vec![b'a'; MAX_ACP_FRAME_BYTES + 1];
        agent_stdout_writer.write_all(&payload).await.unwrap();
        agent_stdout_writer.write_all(b"\n").await.unwrap();
        agent_stdout_writer.flush().await.unwrap();

        let resolved = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx)
            .await
            .expect("pending response should resolve after over-limit frame")
            .expect("pending channel should not be dropped");
        assert_eq!(
            resolved.error.as_ref().map(|e| e.message.as_str()),
            Some("connection closed")
        );
        assert!(pending.lock().await.is_empty());
        handle.await.unwrap();
    }

    #[test]
    fn session_activity_touch_advances_last_active() {
        let activity = SessionActivity::new();
        // Warm the monotonic clock past zero so a backdated value is older.
        std::thread::sleep(std::time::Duration::from_millis(10));
        activity.set_last_active_ms(0);
        let before = activity.last_active_ms();
        activity.touch();
        assert!(activity.last_active_ms() > before);
        // Backdated last_active yields a positive age; touch resets it near zero.
        activity.set_last_active_ms(0);
        assert!(activity.age() >= std::time::Duration::from_millis(10));
        activity.touch();
        assert!(activity.age() < std::time::Duration::from_secs(60));
        // A future timestamp must not underflow: age saturates at zero.
        activity.set_last_active_ms(u64::MAX);
        assert_eq!(activity.age(), std::time::Duration::ZERO);
    }

    #[test]
    fn session_activity_set_in_flight_round_trips() {
        let activity = SessionActivity::new();
        assert!(!activity.in_flight());
        activity.set_in_flight(true);
        assert!(activity.in_flight());
        activity.set_in_flight(false);
        assert!(!activity.in_flight());
    }
}
