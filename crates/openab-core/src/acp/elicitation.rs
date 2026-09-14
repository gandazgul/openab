use crate::acp::protocol::JsonRpcId;
use crate::adapter::{ChannelRef, MessageRef};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{json, Map, Number, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::{oneshot, Mutex};

pub const MAX_ELICITATION_BYTES: usize = 64 * 1024;
pub const MAX_ELICITATION_FIELDS: usize = 50;
pub const MAX_FIELD_CHOICES: usize = 100;
pub const MAX_ACP_FRAME_BYTES: usize = 1024 * 1024;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_TURN_AUTHORITY: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionGeneration(u64);

impl ConnectionGeneration {
    pub fn new() -> Self {
        Self(NEXT_GENERATION.fetch_add(1, Ordering::Relaxed))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl Default for ConnectionGeneration {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ElicitationTurnContext {
    pub authority_id: u64,
    pub session_id: Option<String>,
    pub request_id: Option<u64>,
    pub channel: ChannelRef,
    pub trigger_message: MessageRef,
    pub authorized_user_ids: HashSet<String>,
}

impl ElicitationTurnContext {
    pub fn has_human_authority(&self) -> bool {
        !self.authorized_user_ids.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ElicitationOutcome {
    Accept(Map<String, Value>),
    Decline,
    Cancel,
}

impl ElicitationOutcome {
    pub fn to_result_value(&self) -> Value {
        match self {
            Self::Accept(content) => {
                json!({"action": "accept", "content": Value::Object(content.clone())})
            }
            Self::Decline => json!({"action": "decline"}),
            Self::Cancel => json!({"action": "cancel"}),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElicitationStyle {
    NativeControls,
    TextFallback,
}

#[derive(Debug, Clone)]
pub struct ElicitationPresentation {
    pub nonce: String,
    pub generation: ConnectionGeneration,
    pub(crate) coordinator: Arc<ElicitationCoordinator>,
    pub agent_request_id: JsonRpcId,
    pub agent_name: String,
    pub message: String,
    pub form: FormSchema,
    pub channel: ChannelRef,
    pub trigger_message: MessageRef,
    pub authorized_user_ids: HashSet<String>,
    pub style: ElicitationStyle,
}

#[async_trait]
pub trait FormPresenter: Send + Sync {
    async fn present_form(
        &self,
        presentation: ElicitationPresentation,
    ) -> Result<ElicitationOutcome>;
    async fn expire_form(&self, _nonce: &str, _status: ElicitationStatus) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElicitationStatus {
    Submitted,
    Declined,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormSchema {
    pub fields: Vec<FormField>,
    required: HashSet<String>,
}

impl FormSchema {
    pub fn from_requested_schema(value: &Value) -> std::result::Result<Self, ElicitationError> {
        let obj = value
            .as_object()
            .ok_or_else(|| ElicitationError::invalid_params("requestedSchema must be an object"))?;
        if obj.get("type").and_then(Value::as_str) != Some("object") {
            return Err(ElicitationError::invalid_params(
                "requestedSchema.type must be object",
            ));
        }
        let properties = obj
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ElicitationError::invalid_params("requestedSchema.properties must be an object")
            })?;
        if properties.len() > MAX_ELICITATION_FIELDS {
            return Err(ElicitationError::invalid_params(format!(
                "form has more than {MAX_ELICITATION_FIELDS} fields"
            )));
        }
        let required: HashSet<String> = match obj.get("required") {
            None => HashSet::new(),
            Some(Value::Array(items)) => {
                let mut required = HashSet::new();
                for item in items {
                    let Some(name) = item.as_str() else {
                        return Err(ElicitationError::invalid_params(
                            "requestedSchema.required entries must be strings",
                        ));
                    };
                    if !required.insert(name.to_string()) {
                        return Err(ElicitationError::invalid_params(format!(
                            "required field `{name}` is duplicated"
                        )));
                    }
                }
                required
            }
            Some(_) => {
                return Err(ElicitationError::invalid_params(
                    "requestedSchema.required must be an array",
                ))
            }
        };
        for name in &required {
            if !properties.contains_key(name) {
                return Err(ElicitationError::invalid_params(format!(
                    "required field `{name}` is not in properties"
                )));
            }
        }
        let mut fields = Vec::with_capacity(properties.len());
        for (name, field_value) in properties {
            fields.push(FormField::parse(
                name,
                field_value,
                required.contains(name),
            )?);
        }
        Ok(Self { fields, required })
    }

    pub fn default_content(&self) -> Map<String, Value> {
        let mut content = Map::new();
        for field in &self.fields {
            if let Some(default) = &field.default {
                content.insert(field.name.clone(), default.clone());
            }
        }
        content
    }

    pub fn validate_content(
        &self,
        content: &Map<String, Value>,
    ) -> std::result::Result<(), ElicitationError> {
        for required in &self.required {
            if !content.contains_key(required) || content.get(required).is_some_and(Value::is_null)
            {
                return Err(ElicitationError::invalid_params(format!(
                    "required field `{required}` is missing"
                )));
            }
        }
        for (name, value) in content {
            let field = self
                .fields
                .iter()
                .find(|field| field.name == *name)
                .ok_or_else(|| {
                    ElicitationError::invalid_params(format!("unknown field `{name}`"))
                })?;
            field.validate_value(value)?;
        }
        Ok(())
    }

    pub fn field(&self, name: &str) -> Option<&FormField> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn needs_text_fallback(&self) -> bool {
        self.fields.iter().any(|field| field.needs_text_fallback())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormField {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub required: bool,
    pub kind: FormFieldKind,
    pub default: Option<Value>,
}

impl FormField {
    fn parse(
        name: &str,
        value: &Value,
        required: bool,
    ) -> std::result::Result<Self, ElicitationError> {
        let obj = value.as_object().ok_or_else(|| {
            ElicitationError::invalid_params(format!("field `{name}` must be an object"))
        })?;
        let field_type = obj.get("type").and_then(Value::as_str).ok_or_else(|| {
            ElicitationError::invalid_params(format!("field `{name}` is missing type"))
        })?;
        let title = obj
            .get("title")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let description = obj
            .get("description")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let kind = match field_type {
            "string" => {
                let choices = parse_choices(obj, name)?;
                if choices.is_empty() {
                    let min_length = optional_usize(obj, "minLength", name)?;
                    let max_length = optional_usize(obj, "maxLength", name)?;
                    if let (Some(min), Some(max)) = (min_length, max_length) {
                        if min > max {
                            return Err(ElicitationError::invalid_params(format!(
                                "field `{name}` minLength cannot exceed maxLength"
                            )));
                        }
                    }
                    let format = optional_string(obj, "format", name)?;
                    if let Some(format) = format.as_deref() {
                        validate_known_format(name, format)?;
                    }
                    FormFieldKind::String {
                        min_length,
                        max_length,
                        pattern: optional_regex(obj, "pattern", name)?,
                        format,
                    }
                } else {
                    FormFieldKind::SingleSelect { choices }
                }
            }
            "number" => {
                let minimum = optional_f64(obj, "minimum", name)?;
                let maximum = optional_f64(obj, "maximum", name)?;
                if let (Some(min), Some(max)) = (minimum, maximum) {
                    if min > max {
                        return Err(ElicitationError::invalid_params(format!(
                            "field `{name}` minimum cannot exceed maximum"
                        )));
                    }
                }
                FormFieldKind::Number { minimum, maximum }
            }
            "integer" => {
                let minimum = optional_i64(obj, "minimum", name)?;
                let maximum = optional_i64(obj, "maximum", name)?;
                if let (Some(min), Some(max)) = (minimum, maximum) {
                    if min > max {
                        return Err(ElicitationError::invalid_params(format!(
                            "field `{name}` minimum cannot exceed maximum"
                        )));
                    }
                }
                FormFieldKind::Integer { minimum, maximum }
            }
            "boolean" => FormFieldKind::Boolean,
            "array" => parse_array_kind(name, obj)?,
            other => {
                return Err(ElicitationError::invalid_params(format!(
                    "field `{name}` uses unsupported type `{other}`"
                )))
            }
        };
        let field = Self {
            name: name.to_string(),
            title,
            description,
            required,
            kind,
            default: obj.get("default").cloned(),
        };
        if let Some(default) = &field.default {
            field.validate_value(default)?;
        }
        Ok(field)
    }

    pub fn display_name(&self) -> &str {
        self.title.as_deref().unwrap_or(&self.name)
    }

    pub fn validate_value(&self, value: &Value) -> std::result::Result<(), ElicitationError> {
        match &self.kind {
            FormFieldKind::String {
                min_length,
                max_length,
                pattern,
                format,
            } => {
                let s = value
                    .as_str()
                    .ok_or_else(|| self.invalid("must be a string"))?;
                let len = s.chars().count();
                if min_length.is_some_and(|min| len < min) {
                    return Err(self.invalid(format!(
                        "must have at least {} characters",
                        min_length.unwrap()
                    )));
                }
                if max_length.is_some_and(|max| len > max) {
                    return Err(self.invalid(format!(
                        "must have at most {} characters",
                        max_length.unwrap()
                    )));
                }
                if let Some(pattern) = pattern {
                    let re = regex::Regex::new(pattern)
                        .map_err(|_| self.invalid("has an invalid pattern constraint"))?;
                    if !re.is_match(s) {
                        return Err(self.invalid("does not match the required pattern"));
                    }
                }
                if let Some(format) = format {
                    validate_format(self, format, s)?;
                }
            }
            FormFieldKind::Number { minimum, maximum } => {
                let n = value
                    .as_f64()
                    .ok_or_else(|| self.invalid("must be a number"))?;
                if !n.is_finite() {
                    return Err(self.invalid("must be finite"));
                }
                if minimum.is_some_and(|min| n < min) {
                    return Err(self.invalid(format!("must be at least {}", minimum.unwrap())));
                }
                if maximum.is_some_and(|max| n > max) {
                    return Err(self.invalid(format!("must be at most {}", maximum.unwrap())));
                }
            }
            FormFieldKind::Integer { minimum, maximum } => {
                let n = value
                    .as_i64()
                    .ok_or_else(|| self.invalid("must be an integer"))?;
                if minimum.is_some_and(|min| n < min) {
                    return Err(self.invalid(format!("must be at least {}", minimum.unwrap())));
                }
                if maximum.is_some_and(|max| n > max) {
                    return Err(self.invalid(format!("must be at most {}", maximum.unwrap())));
                }
            }
            FormFieldKind::Boolean => {
                if !value.is_boolean() {
                    return Err(self.invalid("must be a boolean"));
                }
            }
            FormFieldKind::SingleSelect { choices } => {
                let s = value
                    .as_str()
                    .ok_or_else(|| self.invalid("must be a string"))?;
                if !choices.iter().any(|choice| choice.value == s) {
                    return Err(self.invalid("must be one of the declared choices"));
                }
            }
            FormFieldKind::MultiSelect {
                choices,
                min_items,
                max_items,
            } => {
                let items = value
                    .as_array()
                    .ok_or_else(|| self.invalid("must be an array"))?;
                if min_items.is_some_and(|min| items.len() < min) {
                    return Err(self.invalid(format!(
                        "must contain at least {} items",
                        min_items.unwrap()
                    )));
                }
                if max_items.is_some_and(|max| items.len() > max) {
                    return Err(
                        self.invalid(format!("must contain at most {} items", max_items.unwrap()))
                    );
                }
                let mut seen = HashSet::new();
                for item in items {
                    let s = item
                        .as_str()
                        .ok_or_else(|| self.invalid("items must be strings"))?;
                    if !seen.insert(s) {
                        return Err(self.invalid("must not contain duplicate items"));
                    }
                    if !choices.iter().any(|choice| choice.value == s) {
                        return Err(self.invalid("items must be declared choices"));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn parse_user_text(&self, raw: &str) -> std::result::Result<Value, ElicitationError> {
        let value = match &self.kind {
            FormFieldKind::String { .. } => Value::String(raw.to_string()),
            FormFieldKind::Number { .. } => {
                let parsed: f64 = raw
                    .trim()
                    .parse()
                    .map_err(|_| self.invalid("must be a number"))?;
                let number =
                    Number::from_f64(parsed).ok_or_else(|| self.invalid("must be finite"))?;
                Value::Number(number)
            }
            FormFieldKind::Integer { .. } => {
                let parsed: i64 = raw
                    .trim()
                    .parse()
                    .map_err(|_| self.invalid("must be an integer"))?;
                Value::Number(Number::from(parsed))
            }
            FormFieldKind::Boolean => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "yes" | "y" | "1" => Value::Bool(true),
                "false" | "no" | "n" | "0" => Value::Bool(false),
                _ => return Err(self.invalid("must be true or false")),
            },
            FormFieldKind::SingleSelect { choices } => {
                let text = raw.trim();
                let value = choices
                    .iter()
                    .find(|choice| choice.value == text || choice.label.as_deref() == Some(text))
                    .ok_or_else(|| self.invalid("must match a declared choice"))?
                    .value
                    .clone();
                Value::String(value)
            }
            FormFieldKind::MultiSelect { choices, .. } => {
                let trimmed = raw.trim();
                if trimmed.starts_with('[') {
                    let parsed: Vec<String> = serde_json::from_str(trimmed)
                        .map_err(|_| self.invalid("must be a JSON array of choice values"))?;
                    Value::Array(parsed.into_iter().map(Value::String).collect())
                } else {
                    let mut values = Vec::new();
                    for part in raw
                        .split(',')
                        .map(str::trim)
                        .filter(|part| !part.is_empty())
                    {
                        let choice = choices
                            .iter()
                            .find(|choice| {
                                choice.value == part || choice.label.as_deref() == Some(part)
                            })
                            .ok_or_else(|| {
                                self.invalid(format!("`{part}` is not a declared choice"))
                            })?;
                        values.push(Value::String(choice.value.clone()));
                    }
                    Value::Array(values)
                }
            }
        };
        self.validate_value(&value)?;
        Ok(value)
    }

    pub fn needs_text_fallback(&self) -> bool {
        match &self.kind {
            FormFieldKind::SingleSelect { choices }
            | FormFieldKind::MultiSelect { choices, .. } => {
                choices.len() > 25
                    || choices.iter().any(|choice| {
                        choice.value.chars().count() > 100
                            || choice
                                .label
                                .as_ref()
                                .is_some_and(|s| s.chars().count() > 100)
                            || choice
                                .description
                                .as_ref()
                                .is_some_and(|s| s.chars().count() > 100)
                    })
            }
            FormFieldKind::String { .. }
            | FormFieldKind::Number { .. }
            | FormFieldKind::Integer { .. }
            | FormFieldKind::Boolean => self
                .default
                .as_ref()
                .is_some_and(|value| value.to_string().encode_utf16().count() > 4000),
        }
    }

    fn invalid(&self, message: impl Into<String>) -> ElicitationError {
        ElicitationError::invalid_params(format!("field `{}` {}", self.name, message.into()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FormFieldKind {
    String {
        min_length: Option<usize>,
        max_length: Option<usize>,
        pattern: Option<String>,
        format: Option<String>,
    },
    Number {
        minimum: Option<f64>,
        maximum: Option<f64>,
    },
    Integer {
        minimum: Option<i64>,
        maximum: Option<i64>,
    },
    Boolean,
    SingleSelect {
        choices: Vec<FormChoice>,
    },
    MultiSelect {
        choices: Vec<FormChoice>,
        min_items: Option<usize>,
        max_items: Option<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormChoice {
    pub value: String,
    pub label: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ElicitationCreate {
    pub message: String,
    pub form: FormSchema,
}

impl ElicitationCreate {
    pub fn parse(
        params: &Value,
        serialized_len: usize,
        active_session_id: Option<&str>,
        active_request_id: Option<u64>,
    ) -> std::result::Result<Self, ElicitationError> {
        if serialized_len > MAX_ELICITATION_BYTES {
            return Err(ElicitationError::invalid_params(format!(
                "elicitation/create params exceed {MAX_ELICITATION_BYTES} bytes"
            )));
        }
        let obj = params.as_object().ok_or_else(|| {
            ElicitationError::invalid_params("elicitation/create params must be an object")
        })?;
        if obj.get("mode").and_then(Value::as_str) != Some("form") {
            return Err(ElicitationError::invalid_params(
                "only form elicitation is supported",
            ));
        }
        let session_id = optional_string_ref(obj, "sessionId")?;
        let request_id = optional_u64(obj, "requestId", "elicitation/create")?;
        match (session_id, request_id) {
            (Some(session_id), None) => {
                if Some(session_id) != active_session_id {
                    return Err(ElicitationError::invalid_params(
                        "sessionId does not match the active session",
                    ));
                }
                if obj.contains_key("toolCallId")
                    && obj.get("toolCallId").and_then(Value::as_str).is_none()
                {
                    return Err(ElicitationError::invalid_params(
                        "toolCallId must be a string",
                    ));
                }
            }
            (None, Some(request_id)) => {
                if Some(request_id) != active_request_id {
                    return Err(ElicitationError::invalid_params(
                        "requestId does not match the active request",
                    ));
                }
                if obj.contains_key("toolCallId") {
                    return Err(ElicitationError::invalid_params(
                        "toolCallId requires sessionId scope",
                    ));
                }
            }
            (Some(_), Some(_)) => {
                return Err(ElicitationError::invalid_params(
                    "set exactly one of sessionId or requestId",
                ));
            }
            (None, None) => {
                return Err(ElicitationError::invalid_params(
                    "set exactly one of sessionId or requestId",
                ));
            }
        }
        let message = obj
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("The Agent requests more information.")
            .to_string();
        let requested_schema = obj
            .get("requestedSchema")
            .ok_or_else(|| ElicitationError::invalid_params("requestedSchema is required"))?;
        let form = FormSchema::from_requested_schema(requested_schema)?;
        Ok(Self { message, form })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElicitationError {
    pub code: i64,
    pub message: String,
}

impl ElicitationError {
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    pub fn busy() -> Self {
        Self {
            code: -32000,
            message: "elicitation already pending".into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32603,
            message: message.into(),
        }
    }

    pub fn to_error_value(&self) -> Value {
        json!({"code": self.code, "message": self.message})
    }
}

#[derive(Debug)]
pub enum ElicitationStart {
    Present(Box<ElicitationLease>),
    Immediate(ElicitationOutcome),
    Error(ElicitationError),
}

#[derive(Debug)]
pub struct ElicitationLease {
    pub presentation: ElicitationPresentation,
    receiver: oneshot::Receiver<ElicitationOutcome>,
}

impl ElicitationLease {
    pub fn into_parts(
        self,
    ) -> (
        ElicitationPresentation,
        oneshot::Receiver<ElicitationOutcome>,
    ) {
        (self.presentation, self.receiver)
    }

    pub async fn wait(self) -> ElicitationOutcome {
        self.receiver.await.unwrap_or(ElicitationOutcome::Cancel)
    }
}

#[derive(Default)]
struct CoordinatorState {
    pending: HashMap<ConnectionGeneration, PendingElicitation>,
}

struct PendingElicitation {
    nonce: String,
    channel: ChannelRef,
    trigger_message: MessageRef,
    presentation_message_id: Option<String>,
    authorized_user_ids: HashSet<String>,
    sender: Option<oneshot::Sender<ElicitationOutcome>>,
}

#[derive(Default)]
pub struct ElicitationCoordinator {
    state: Mutex<CoordinatorState>,
    revoked: StdMutex<HashSet<ConnectionGeneration>>,
    active_turns: StdMutex<HashMap<ConnectionGeneration, u64>>,
}

impl std::fmt::Debug for ElicitationCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElicitationCoordinator")
            .finish_non_exhaustive()
    }
}

impl ElicitationCoordinator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn revoke_generation(&self, generation: ConnectionGeneration) {
        self.active_turns
            .lock()
            .expect("elicitation active turns lock poisoned")
            .remove(&generation);
        self.revoked
            .lock()
            .expect("elicitation revoked lock poisoned")
            .insert(generation);
    }

    pub fn open_generation_turn(&self, generation: ConnectionGeneration) -> u64 {
        let authority_id = NEXT_TURN_AUTHORITY.fetch_add(1, Ordering::Relaxed);
        self.revoked
            .lock()
            .expect("elicitation revoked lock poisoned")
            .remove(&generation);
        self.active_turns
            .lock()
            .expect("elicitation active turns lock poisoned")
            .insert(generation, authority_id);
        authority_id
    }

    fn is_active_turn(&self, generation: ConnectionGeneration, authority_id: u64) -> bool {
        self.active_turns
            .lock()
            .expect("elicitation active turns lock poisoned")
            .get(&generation)
            .is_some_and(|active| *active == authority_id)
    }

    fn is_revoked(&self, generation: ConnectionGeneration) -> bool {
        self.revoked
            .lock()
            .expect("elicitation revoked lock poisoned")
            .contains(&generation)
    }

    pub async fn start(
        self: &Arc<Self>,
        generation: ConnectionGeneration,
        agent_request_id: JsonRpcId,
        params: &Value,
        serialized_len: usize,
        turn: Option<&ElicitationTurnContext>,
        agent_name: &str,
    ) -> ElicitationStart {
        if self.is_revoked(generation) {
            return ElicitationStart::Immediate(ElicitationOutcome::Cancel);
        }
        let Some(turn) = turn else {
            return ElicitationStart::Immediate(ElicitationOutcome::Cancel);
        };
        if !turn.has_human_authority() || !self.is_active_turn(generation, turn.authority_id) {
            return ElicitationStart::Immediate(ElicitationOutcome::Cancel);
        }
        let create = match ElicitationCreate::parse(
            params,
            serialized_len,
            turn.session_id.as_deref(),
            turn.request_id,
        ) {
            Ok(create) => create,
            Err(err) => return ElicitationStart::Error(err),
        };

        let mut state = self.state.lock().await;
        if self.is_revoked(generation) || !self.is_active_turn(generation, turn.authority_id) {
            return ElicitationStart::Immediate(ElicitationOutcome::Cancel);
        }
        if state.pending.contains_key(&generation) {
            return ElicitationStart::Error(ElicitationError::busy());
        }
        let (sender, receiver) = oneshot::channel();
        let nonce = uuid::Uuid::new_v4().to_string();
        let presentation = ElicitationPresentation {
            nonce: nonce.clone(),
            generation,
            coordinator: self.clone(),
            agent_request_id: agent_request_id.clone(),
            agent_name: if agent_name.is_empty() {
                "Agent Runtime".to_string()
            } else {
                agent_name.to_string()
            },
            message: create.message,
            style: if create.form.needs_text_fallback() {
                ElicitationStyle::TextFallback
            } else {
                ElicitationStyle::NativeControls
            },
            form: create.form,
            channel: turn.channel.clone(),
            trigger_message: turn.trigger_message.clone(),
            authorized_user_ids: turn.authorized_user_ids.clone(),
        };
        state.pending.insert(
            generation,
            PendingElicitation {
                nonce,
                channel: turn.channel.clone(),
                trigger_message: turn.trigger_message.clone(),
                presentation_message_id: None,
                authorized_user_ids: turn.authorized_user_ids.clone(),
                sender: Some(sender),
            },
        );
        ElicitationStart::Present(Box::new(ElicitationLease {
            presentation,
            receiver,
        }))
    }

    pub async fn resolve(
        &self,
        generation: ConnectionGeneration,
        nonce: &str,
        user_id: &str,
        channel: &ChannelRef,
        reply_to_message_id: Option<&str>,
        outcome: ElicitationOutcome,
    ) -> std::result::Result<(), ElicitationError> {
        if self.is_revoked(generation) {
            return Err(ElicitationError::invalid_params(
                "elicitation is no longer pending",
            ));
        }
        let mut state = self.state.lock().await;
        let pending = state
            .pending
            .get_mut(&generation)
            .ok_or_else(|| ElicitationError::invalid_params("elicitation is no longer pending"))?;
        if pending.nonce != nonce {
            return Err(ElicitationError::invalid_params("stale elicitation action"));
        }
        if &pending.channel != channel {
            return Err(ElicitationError::invalid_params(
                "elicitation action came from the wrong channel",
            ));
        }
        if !pending.authorized_user_ids.contains(user_id) {
            return Err(ElicitationError::invalid_params(
                "user is not authorized for this elicitation",
            ));
        }
        if let Some(reply_to_message_id) = reply_to_message_id {
            let matches_trigger = pending.trigger_message.message_id == reply_to_message_id;
            let matches_form = pending
                .presentation_message_id
                .as_deref()
                .is_some_and(|message_id| message_id == reply_to_message_id);
            if !matches_trigger && !matches_form {
                return Err(ElicitationError::invalid_params(
                    "text reply does not target this elicitation",
                ));
            }
        }
        let Some(sender) = pending.sender.take() else {
            return Err(ElicitationError::invalid_params(
                "elicitation is already resolved",
            ));
        };
        let _ = sender.send(outcome);
        Ok(())
    }

    pub async fn bind_message_id(
        &self,
        generation: ConnectionGeneration,
        nonce: &str,
        channel: &ChannelRef,
        message_id: String,
    ) -> bool {
        if self.is_revoked(generation) {
            return false;
        }
        let mut state = self.state.lock().await;
        let Some(pending) = state.pending.get_mut(&generation) else {
            return false;
        };
        if pending.nonce != nonce || &pending.channel != channel || pending.sender.is_none() {
            return false;
        }
        pending.presentation_message_id = Some(message_id);
        true
    }

    pub async fn complete_generation(&self, generation: ConnectionGeneration, nonce: &str) -> bool {
        if self.is_revoked(generation) {
            return false;
        }
        let mut state = self.state.lock().await;
        let Some(pending) = state.pending.get_mut(&generation) else {
            return false;
        };
        if pending.nonce != nonce {
            return false;
        }
        pending.sender.take().is_some()
    }

    pub async fn expire_generation_nonce(
        &self,
        generation: ConnectionGeneration,
        nonce: &str,
        outcome: ElicitationOutcome,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some(pending) = state.pending.get(&generation) else {
            return false;
        };
        if pending.nonce != nonce {
            return false;
        }
        let Some(mut pending) = state.pending.remove(&generation) else {
            return false;
        };
        if let Some(sender) = pending.sender.take() {
            let _ = sender.send(outcome);
        }
        true
    }

    pub async fn finish_generation(&self, generation: ConnectionGeneration, nonce: &str) -> bool {
        let mut state = self.state.lock().await;
        if state
            .pending
            .get(&generation)
            .is_some_and(|pending| pending.nonce == nonce)
        {
            state.pending.remove(&generation);
            true
        } else {
            false
        }
    }

    pub async fn expire_generation(
        &self,
        generation: ConnectionGeneration,
        outcome: ElicitationOutcome,
    ) -> bool {
        self.revoke_generation(generation);
        let mut state = self.state.lock().await;
        let Some(mut pending) = state.pending.remove(&generation) else {
            return false;
        };
        if let Some(sender) = pending.sender.take() {
            let _ = sender.send(outcome);
        }
        true
    }

    pub async fn expire_all(&self) -> usize {
        let mut state = self.state.lock().await;
        let pending = std::mem::take(&mut state.pending);
        let count = pending.len();
        for (generation, mut item) in pending {
            self.revoke_generation(generation);
            if let Some(sender) = item.sender.take() {
                let _ = sender.send(ElicitationOutcome::Cancel);
            }
        }
        count
    }

    pub async fn active_count(&self) -> usize {
        self.state.lock().await.pending.len()
    }

    pub async fn is_authorized_pending(
        &self,
        generation: ConnectionGeneration,
        nonce: &str,
        user_id: &str,
        channel: &ChannelRef,
    ) -> bool {
        if self.is_revoked(generation) {
            return false;
        }
        let state = self.state.lock().await;
        state.pending.get(&generation).is_some_and(|pending| {
            pending.nonce == nonce
                && &pending.channel == channel
                && pending.sender.is_some()
                && pending.authorized_user_ids.contains(user_id)
        })
    }
}

fn optional_string(
    obj: &Map<String, Value>,
    key: &str,
    field_name: &str,
) -> std::result::Result<Option<String>, ElicitationError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` {key} must be a string"
        ))),
    }
}

fn optional_string_ref<'a>(
    obj: &'a Map<String, Value>,
    key: &str,
) -> std::result::Result<Option<&'a str>, ElicitationError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(ElicitationError::invalid_params(format!(
            "{key} must be a string"
        ))),
    }
}

fn optional_usize(
    obj: &Map<String, Value>,
    key: &str,
    field_name: &str,
) -> std::result::Result<Option<usize>, ElicitationError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .map(Some)
            .ok_or_else(|| {
                ElicitationError::invalid_params(format!(
                    "field `{field_name}` {key} must be a non-negative integer"
                ))
            }),
        Some(_) => Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` {key} must be a non-negative integer"
        ))),
    }
}

fn optional_u64(
    obj: &Map<String, Value>,
    key: &str,
    context: &str,
) -> std::result::Result<Option<u64>, ElicitationError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
            ElicitationError::invalid_params(format!(
                "{context} {key} must be a non-negative integer"
            ))
        }),
        Some(_) => Err(ElicitationError::invalid_params(format!(
            "{context} {key} must be a non-negative integer"
        ))),
    }
}

fn optional_i64(
    obj: &Map<String, Value>,
    key: &str,
    field_name: &str,
) -> std::result::Result<Option<i64>, ElicitationError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n.as_i64().map(Some).ok_or_else(|| {
            ElicitationError::invalid_params(format!(
                "field `{field_name}` {key} must be an integer"
            ))
        }),
        Some(_) => Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` {key} must be an integer"
        ))),
    }
}

fn optional_f64(
    obj: &Map<String, Value>,
    key: &str,
    field_name: &str,
) -> std::result::Result<Option<f64>, ElicitationError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n
            .as_f64()
            .filter(|v| v.is_finite())
            .map(Some)
            .ok_or_else(|| {
                ElicitationError::invalid_params(format!(
                    "field `{field_name}` {key} must be a finite number"
                ))
            }),
        Some(_) => Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` {key} must be a finite number"
        ))),
    }
}

fn optional_regex(
    obj: &Map<String, Value>,
    key: &str,
    field_name: &str,
) -> std::result::Result<Option<String>, ElicitationError> {
    let Some(pattern) = optional_string(obj, key, field_name)? else {
        return Ok(None);
    };
    regex::Regex::new(&pattern).map_err(|_| {
        ElicitationError::invalid_params(format!("field `{field_name}` has an invalid pattern"))
    })?;
    Ok(Some(pattern))
}

fn parse_array_kind(
    name: &str,
    obj: &Map<String, Value>,
) -> std::result::Result<FormFieldKind, ElicitationError> {
    let items = obj.get("items").and_then(Value::as_object).ok_or_else(|| {
        ElicitationError::invalid_params(format!("field `{name}` array items must be an object"))
    })?;
    let choices = parse_choices(items, name)?;
    if items
        .get("type")
        .is_some_and(|item_type| item_type.as_str() != Some("string"))
    {
        return Err(ElicitationError::invalid_params(format!(
            "field `{name}` only supports string array items"
        )));
    }
    if choices.is_empty() {
        return Err(ElicitationError::invalid_params(format!(
            "field `{name}` array must declare string enum choices"
        )));
    }
    let min_items = optional_usize(obj, "minItems", name)?;
    let max_items = optional_usize(obj, "maxItems", name)?;
    if let (Some(min), Some(max)) = (min_items, max_items) {
        if min > max {
            return Err(ElicitationError::invalid_params(format!(
                "field `{name}` minItems cannot exceed maxItems"
            )));
        }
    }
    Ok(FormFieldKind::MultiSelect {
        choices,
        min_items,
        max_items,
    })
}

fn parse_choices(
    obj: &Map<String, Value>,
    field_name: &str,
) -> std::result::Result<Vec<FormChoice>, ElicitationError> {
    let source = if let Some(enum_value) = obj.get("enum") {
        let values = enum_value.as_array().ok_or_else(|| {
            ElicitationError::invalid_params(format!("field `{field_name}` enum must be an array"))
        })?;
        if values.is_empty() {
            return Err(ElicitationError::invalid_params(format!(
                "field `{field_name}` enum must not be empty"
            )));
        }
        ChoiceSource::Enum(values)
    } else if let Some(one_of) = obj.get("oneOf") {
        ChoiceSource::ConstList(one_of.as_array().ok_or_else(|| {
            ElicitationError::invalid_params(format!("field `{field_name}` oneOf must be an array"))
        })?)
    } else if let Some(any_of) = obj.get("anyOf") {
        ChoiceSource::ConstList(any_of.as_array().ok_or_else(|| {
            ElicitationError::invalid_params(format!("field `{field_name}` anyOf must be an array"))
        })?)
    } else {
        return Ok(Vec::new());
    };
    let len = source.len();
    if len == 0 {
        return Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` choices must not be empty"
        )));
    }
    if len > MAX_FIELD_CHOICES {
        return Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` has more than {MAX_FIELD_CHOICES} choices"
        )));
    }
    let mut choices = Vec::with_capacity(len);
    let mut seen = HashSet::new();
    for value in source.values() {
        let (wire, label, description) = match (source, value) {
            (ChoiceSource::Enum(_), Value::String(s)) => (s.as_str(), None, None),
            (ChoiceSource::Enum(_), Value::Object(obj)) => {
                let Some(wire) = obj.get("value").and_then(Value::as_str) else {
                    return Err(ElicitationError::invalid_params(format!(
                        "field `{field_name}` titled enum values must include a string value"
                    )));
                };
                let label = optional_string(obj, "label", field_name)?;
                let description = optional_string(obj, "description", field_name)?;
                (wire, label, description)
            }
            (ChoiceSource::ConstList(_), Value::Object(obj)) => {
                let Some(wire) = obj.get("const").and_then(Value::as_str) else {
                    return Err(ElicitationError::invalid_params(format!(
                        "field `{field_name}` oneOf/anyOf choices must include a string const"
                    )));
                };
                let label = optional_string(obj, "title", field_name)?;
                let description = optional_string(obj, "description", field_name)?;
                (wire, label, description)
            }
            (ChoiceSource::Enum(_), _) => {
                return Err(ElicitationError::invalid_params(format!(
                    "field `{field_name}` enum values must be strings or titled choice objects"
                )))
            }
            (ChoiceSource::ConstList(_), _) => {
                return Err(ElicitationError::invalid_params(format!(
                    "field `{field_name}` oneOf/anyOf choices must be objects"
                )))
            }
        };
        if !seen.insert(wire.to_string()) {
            return Err(ElicitationError::invalid_params(format!(
                "field `{field_name}` choice values must be unique"
            )));
        }
        choices.push(FormChoice {
            value: wire.to_string(),
            label,
            description,
        });
    }
    Ok(choices)
}

#[derive(Clone, Copy)]
enum ChoiceSource<'a> {
    Enum(&'a [Value]),
    ConstList(&'a [Value]),
}

impl<'a> ChoiceSource<'a> {
    fn len(self) -> usize {
        match self {
            Self::Enum(values) | Self::ConstList(values) => values.len(),
        }
    }

    fn values(self) -> std::slice::Iter<'a, Value> {
        match self {
            Self::Enum(values) | Self::ConstList(values) => values.iter(),
        }
    }
}

fn validate_known_format(
    field_name: &str,
    format: &str,
) -> std::result::Result<(), ElicitationError> {
    match format {
        "email" | "uri" | "url" | "date-time" | "date" => Ok(()),
        other => Err(ElicitationError::invalid_params(format!(
            "field `{field_name}` uses unsupported format `{other}`"
        ))),
    }
}

fn is_valid_email(s: &str) -> bool {
    let Some((local, domain)) = s.split_once('@') else {
        return false;
    };
    if local.is_empty()
        || domain.is_empty()
        || domain.contains('@')
        || local.starts_with('.')
        || local.ends_with('.')
        || local.contains("..")
        || domain.starts_with('.')
        || domain.ends_with('.')
        || domain.contains("..")
    {
        return false;
    }
    if s.chars().any(|ch| ch.is_whitespace() || ch.is_control()) {
        return false;
    }
    let labels: Vec<_> = domain.split('.').collect();
    labels.len() >= 2
        && labels.iter().all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
        })
}

fn validate_format(
    field: &FormField,
    format: &str,
    s: &str,
) -> std::result::Result<(), ElicitationError> {
    match format {
        "email" => {
            if !is_valid_email(s) {
                return Err(field.invalid("must be an email address"));
            }
        }
        "uri" | "url" => {
            let parsed = reqwest::Url::parse(s).map_err(|_| field.invalid("must be a URI"))?;
            if parsed.scheme().is_empty() {
                return Err(field.invalid("must be a URI"));
            }
        }
        "date-time" => {
            chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|_| field.invalid("must be an RFC 3339 date-time"))?;
        }
        "date" => {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| field.invalid("must be an RFC 3339 full-date"))?;
        }
        other => {
            return Err(field.invalid(format!("uses unsupported format `{other}`")));
        }
    }
    Ok(())
}

pub fn jsonrpc_success(id: JsonRpcId, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub fn jsonrpc_error(id: JsonRpcId, err: ElicitationError) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": err.to_error_value()})
}

pub fn presentation_failure(e: anyhow::Error) -> ElicitationError {
    ElicitationError::internal(format!("elicitation presentation failed: {e}"))
}

pub fn validate_accepted_content(
    form: &FormSchema,
    outcome: ElicitationOutcome,
) -> std::result::Result<ElicitationOutcome, ElicitationError> {
    if let ElicitationOutcome::Accept(content) = &outcome {
        form.validate_content(content)?;
    }
    Ok(outcome)
}

pub fn invalid_outcome_error(err: ElicitationError) -> anyhow::Error {
    anyhow!(err.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::ChannelRef;

    fn schema(properties: Map<String, Value>, required: Vec<&str>) -> Value {
        json!({
            "type": "object",
            "properties": Value::Object(properties),
            "required": required,
        })
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

    fn turn() -> ElicitationTurnContext {
        let ch = channel();
        ElicitationTurnContext {
            authority_id: 0,
            session_id: Some("sess".into()),
            request_id: Some(7),
            channel: ch.clone(),
            trigger_message: MessageRef {
                channel: ch,
                message_id: "99".into(),
            },
            authorized_user_ids: HashSet::from(["u1".into(), "u2".into()]),
        }
    }

    #[test]
    fn parses_supported_field_types_and_defaults() {
        let mut props = Map::new();
        props.insert(
            "name".into(),
            json!({"type":"string","minLength":2,"default":"Ada"}),
        );
        props.insert(
            "count".into(),
            json!({"type":"integer","minimum":1,"maximum":5,"default":3}),
        );
        props.insert(
            "ratio".into(),
            json!({"type":"number","minimum":0,"maximum":1,"default":0.5}),
        );
        props.insert("ok".into(), json!({"type":"boolean","default":true}));
        props.insert(
            "mode".into(),
            json!({"type":"string","enum":["a","b"],"default":"a"}),
        );
        props.insert("tags".into(), json!({"type":"array","items":{"type":"string","enum":["x","y"]},"minItems":1,"maxItems":2,"default":["x"]}));
        let form = FormSchema::from_requested_schema(&schema(props, vec!["name", "mode"])).unwrap();
        assert_eq!(form.fields.len(), 6);
        form.validate_content(&form.default_content()).unwrap();
    }

    #[test]
    fn rejects_invalid_scope_mode_size_and_shape() {
        let params = json!({"sessionId":"other","mode":"form","message":"m","requestedSchema":{"type":"object","properties":{}}});
        let err = ElicitationCreate::parse(&params, 100, Some("sess"), Some(7)).unwrap_err();
        assert_eq!(err.code, -32602);
        let params = json!({"requestId":7,"mode":"url","url":"https://example.com"});
        assert_eq!(
            ElicitationCreate::parse(&params, 100, Some("sess"), Some(7))
                .unwrap_err()
                .code,
            -32602
        );
        let params = json!({"requestId":7,"mode":"form","requestedSchema":{"type":"object","properties":{}}});
        ElicitationCreate::parse(&params, MAX_ELICITATION_BYTES, Some("sess"), Some(7)).unwrap();
        assert_eq!(
            ElicitationCreate::parse(&params, MAX_ELICITATION_BYTES + 1, Some("sess"), Some(7))
                .unwrap_err()
                .code,
            -32602
        );
    }

    #[test]
    fn enforces_field_and_choice_bounds() {
        let mut props = Map::new();
        for i in 0..MAX_ELICITATION_FIELDS {
            props.insert(format!("f{i}"), json!({"type":"string"}));
        }
        FormSchema::from_requested_schema(&schema(props, vec![])).unwrap();

        let mut props = Map::new();
        for i in 0..=MAX_ELICITATION_FIELDS {
            props.insert(format!("f{i}"), json!({"type":"string"}));
        }
        assert_eq!(
            FormSchema::from_requested_schema(&schema(props, vec![]))
                .unwrap_err()
                .code,
            -32602
        );

        let choices: Vec<String> = (0..MAX_FIELD_CHOICES).map(|i| format!("c{i}")).collect();
        let mut props = Map::new();
        props.insert("choice".into(), json!({"type":"string","enum":choices}));
        FormSchema::from_requested_schema(&schema(props, vec![])).unwrap();

        let choices: Vec<String> = (0..=MAX_FIELD_CHOICES).map(|i| format!("c{i}")).collect();
        let mut props = Map::new();
        props.insert("choice".into(), json!({"type":"string","enum":choices}));
        assert_eq!(
            FormSchema::from_requested_schema(&schema(props, vec![]))
                .unwrap_err()
                .code,
            -32602
        );
    }

    #[test]
    fn validates_accepted_content_and_user_text() {
        let mut props = Map::new();
        props.insert("email".into(), json!({"type":"string","format":"email"}));
        props.insert("qty".into(), json!({"type":"integer","minimum":2}));
        let form = FormSchema::from_requested_schema(&schema(props, vec!["email", "qty"])).unwrap();
        let mut content = Map::new();
        content.insert("email".into(), json!("bad"));
        content.insert("qty".into(), json!(1));
        assert!(form.validate_content(&content).is_err());
        content.insert("email".into(), json!("a@example.com"));
        content.insert("qty".into(), json!(2));
        form.validate_content(&content).unwrap();
        assert_eq!(
            form.field("qty").unwrap().parse_user_text("3").unwrap(),
            json!(3)
        );
    }

    #[test]
    fn parses_titled_oneof_and_anyof_choices() {
        let mut props = Map::new();
        props.insert(
            "mode".into(),
            json!({"type":"string","oneOf":[{"const":"fast","title":"Fast"},{"const":"safe","title":"Safe","description":"Careful"}],"default":"safe"}),
        );
        props.insert(
            "tags".into(),
            json!({"type":"array","items":{"anyOf":[{"const":"a","title":"A"},{"const":"b","title":"B"}]},"default":["a"]}),
        );
        let form = FormSchema::from_requested_schema(&schema(props, vec![])).unwrap();
        assert!(matches!(
            form.field("mode").unwrap().kind,
            FormFieldKind::SingleSelect { .. }
        ));
        assert!(matches!(
            form.field("tags").unwrap().kind,
            FormFieldKind::MultiSelect { .. }
        ));
        form.validate_content(&form.default_content()).unwrap();

        let mut props = Map::new();
        props.insert(
            "bad".into(),
            json!({"type":"array","items":{"type":"integer","enum":["a"]}}),
        );
        assert!(FormSchema::from_requested_schema(&schema(props, vec![])).is_err());
    }

    #[test]
    fn validates_formats_and_rejects_malformed_constraints() {
        let mut props = Map::new();
        props.insert(
            "when".into(),
            json!({"type":"string","format":"date","default":"2026-09-12"}),
        );
        props.insert(
            "at".into(),
            json!({"type":"string","format":"date-time","default":"2026-09-12T10:00:00Z"}),
        );
        props.insert("email".into(), json!({"type":"string","format":"email"}));
        let form = FormSchema::from_requested_schema(&schema(props, vec![])).unwrap();
        form.validate_content(&form.default_content()).unwrap();
        assert!(form
            .field("email")
            .unwrap()
            .validate_value(&json!("a b@c.com"))
            .is_err());
        assert!(form
            .field("email")
            .unwrap()
            .validate_value(&json!("a@b@c.com"))
            .is_err());
        assert!(form
            .field("email")
            .unwrap()
            .validate_value(&json!("a..b@c.com"))
            .is_err());
        assert!(form
            .field("email")
            .unwrap()
            .validate_value(&json!("a@b..com"))
            .is_err());

        let mut props = Map::new();
        props.insert("empty".into(), json!({"type":"string","enum":[]}));
        assert!(FormSchema::from_requested_schema(&schema(props, vec![])).is_err());
        let mut props = Map::new();
        props.insert(
            "bad_format".into(),
            json!({"type":"string","format":"hostname"}),
        );
        assert!(FormSchema::from_requested_schema(&schema(props, vec![])).is_err());
        let mut props = Map::new();
        props.insert(
            "bad_len".into(),
            json!({"type":"string","minLength":5,"maxLength":3}),
        );
        assert!(FormSchema::from_requested_schema(&schema(props, vec![])).is_err());
        let mut props = Map::new();
        props.insert(
            "bad_num".into(),
            json!({"type":"number","minimum":10,"maximum":1}),
        );
        assert!(FormSchema::from_requested_schema(&schema(props, vec![])).is_err());
    }

    #[tokio::test]
    async fn coordinator_allows_one_pending_and_authorized_resolution() {
        let coord = ElicitationCoordinator::new();
        let gen = ConnectionGeneration::new();
        let params = json!({"sessionId":"sess","mode":"form","message":"m","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}});
        let mut active_turn = turn();
        active_turn.authority_id = coord.open_generation_turn(gen);
        let start = coord
            .start(
                gen,
                JsonRpcId::Number(5),
                &params,
                200,
                Some(&active_turn),
                "agent",
            )
            .await;
        let ElicitationStart::Present(lease) = start else {
            panic!("expected lease")
        };
        assert_eq!(coord.active_count().await, 1);
        let busy = coord
            .start(
                gen,
                JsonRpcId::Number(6),
                &params,
                200,
                Some(&active_turn),
                "agent",
            )
            .await;
        assert!(matches!(
            busy,
            ElicitationStart::Error(ElicitationError { code: -32000, .. })
        ));
        let nonce = lease.presentation.nonce.clone();
        let mut content = Map::new();
        content.insert("name".into(), json!("Ada"));
        coord
            .resolve(
                gen,
                &nonce,
                "u1",
                &channel(),
                Some("99"),
                ElicitationOutcome::Accept(content.clone()),
            )
            .await
            .unwrap();
        assert_eq!(lease.wait().await, ElicitationOutcome::Accept(content));
        assert_eq!(coord.active_count().await, 1);
        assert!(coord.finish_generation(gen, &nonce).await);
        assert_eq!(coord.active_count().await, 0);
    }

    #[tokio::test]
    async fn coordinator_rejects_unauthorized_and_stale_resolution() {
        let coord = ElicitationCoordinator::new();
        let gen = ConnectionGeneration::new();
        let params = json!({"requestId":7,"mode":"form","requestedSchema":{"type":"object","properties":{}}});
        let mut active_turn = turn();
        active_turn.authority_id = coord.open_generation_turn(gen);
        let start = coord
            .start(
                gen,
                JsonRpcId::String("s".into()),
                &params,
                200,
                Some(&active_turn),
                "agent",
            )
            .await;
        let ElicitationStart::Present(lease) = start else {
            panic!("expected lease")
        };
        let nonce = lease.presentation.nonce.clone();
        assert!(coord
            .resolve(
                ConnectionGeneration::new(),
                &nonce,
                "u1",
                &channel(),
                Some("99"),
                ElicitationOutcome::Cancel
            )
            .await
            .is_err());
        assert!(coord
            .resolve(
                gen,
                &nonce,
                "other",
                &channel(),
                Some("99"),
                ElicitationOutcome::Cancel
            )
            .await
            .is_err());
        let mut wrong_channel = channel();
        wrong_channel.channel_id = "11".into();
        assert!(coord
            .resolve(
                gen,
                &nonce,
                "u1",
                &wrong_channel,
                Some("99"),
                ElicitationOutcome::Cancel
            )
            .await
            .is_err());
        assert!(coord
            .resolve(
                gen,
                &nonce,
                "u1",
                &channel(),
                Some("100"),
                ElicitationOutcome::Cancel
            )
            .await
            .is_err());
        assert_eq!(coord.active_count().await, 1);
        coord
            .expire_generation(gen, ElicitationOutcome::Cancel)
            .await;
        assert_eq!(lease.wait().await, ElicitationOutcome::Cancel);
        assert!(
            !coord
                .is_authorized_pending(gen, &nonce, "u1", &channel())
                .await
        );
        assert!(coord
            .resolve(
                gen,
                &nonce,
                "u1",
                &channel(),
                Some("99"),
                ElicitationOutcome::Decline
            )
            .await
            .is_err());
    }
    #[tokio::test]
    async fn revoked_generation_rejects_copied_turn_after_next_prompt_reauthorizes() {
        let coord = ElicitationCoordinator::new();
        let gen = ConnectionGeneration::new();
        let params = json!({"requestId":7,"mode":"form","requestedSchema":{"type":"object","properties":{}}});
        let mut copied_turn = turn();
        copied_turn.authority_id = coord.open_generation_turn(gen);
        coord.revoke_generation(gen);
        assert!(matches!(
            coord
                .start(
                    gen,
                    JsonRpcId::Number(1),
                    &params,
                    200,
                    Some(&copied_turn),
                    "agent"
                )
                .await,
            ElicitationStart::Immediate(ElicitationOutcome::Cancel)
        ));
        let mut next_turn = turn();
        next_turn.authority_id = coord.open_generation_turn(gen);
        assert!(matches!(
            coord
                .start(
                    gen,
                    JsonRpcId::Number(2),
                    &params,
                    200,
                    Some(&copied_turn),
                    "agent"
                )
                .await,
            ElicitationStart::Immediate(ElicitationOutcome::Cancel)
        ));
        assert!(matches!(
            coord
                .start(
                    gen,
                    JsonRpcId::Number(3),
                    &params,
                    200,
                    Some(&next_turn),
                    "agent"
                )
                .await,
            ElicitationStart::Present(_)
        ));
    }
}
