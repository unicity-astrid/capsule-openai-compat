#![deny(unsafe_code)]
#![deny(clippy::all)]
#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! OpenAI-compatible LLM provider capsule.
//!
//! Subscribes to `llm.v1.request.generate.openai-compat` IPC events, calls any
//! OpenAI-compatible Chat Completions API via the HTTP airlock, parses the SSE
//! streaming response, and publishes standardized `llm.v1.stream.openai-compat`
//! events back to the event bus.
//!
//! Configure `base_url` to point at any compatible provider:
//! - OpenAI: `https://api.openai.com`
//! - Groq: `https://api.groq.com/openai`
//! - Together: `https://api.together.ai`
//! - Mistral: `https://api.mistral.ai`
//! - DeepSeek: `https://api.deepseek.com`
//! - Fireworks: `https://api.fireworks.ai/inference`

mod schemas;

use astrid_sdk::prelude::*;
use astrid_sdk::types::{IpcPayload, Message, MessageContent, MessageRole, StreamEvent};
use schemas::{ChatCompletionChunk, ModelList};
use serde_json::Value;
use uuid::Uuid;

const STREAM_TOPIC: &str = "llm.v1.stream.openai-compat";
/// Maximum SSE line buffer size (1 MB). If the server sends data without
/// a newline that exceeds this, the stream is aborted.
const MAX_LINE_BUFFER_SIZE: usize = 1024 * 1024;

/// OpenAI-compatible LLM provider capsule.
#[derive(Default)]
pub struct OpenAICompatProvider;

#[capsule]
impl OpenAICompatProvider {
    /// Handles incoming LLM generation requests.
    #[astrid::interceptor("handle_llm_request")]
    pub fn handle_llm_request(&self, req: IpcPayload) -> Result<(), SysError> {
        if let IpcPayload::LlmRequest {
            request_id,
            model,
            messages,
            tools,
            system,
            ..
        } = req
            && let Err(e) = Self::execute_request(request_id, &model, &messages, &tools, &system)
        {
            log::error(format!("LLM request failed: {e}"));
            let _ = ipc::publish_json(
                STREAM_TOPIC,
                &IpcPayload::LlmStreamEvent {
                    request_id,
                    event: StreamEvent::Error(e.to_string()),
                },
            );
        }
        Ok(())
    }

    /// Returns provider metadata for IPC-based provider discovery.
    ///
    /// The registry capsule publishes a `llm.v1.request.describe` envelope
    /// and drains responses on `llm.v1.response.describe` for a bounded
    /// window. Each provider capsule subscribes to the request topic and
    /// publishes its capability descriptor on the response topic. This
    /// replaces the pre-#752 `hooks::trigger` fan-out path that returned
    /// interceptor results through kernel-mediated dispatch — under the
    /// new ABI the interceptor return value is no longer fanned out, so
    /// the provider must publish explicitly.
    ///
    /// The return value is kept (same shape) so other interceptor callers
    /// continue to see the descriptor; the explicit `ipc::publish_json`
    /// is what registry's new fan-out actually consumes.
    #[astrid::interceptor("llm_describe")]
    pub fn llm_describe(&self, _payload: serde_json::Value) -> Result<serde_json::Value, SysError> {
        let default_model = env::var("model").unwrap_or_else(|_| "unknown".into());
        let context_window = env::var("context_window")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(128_000);
        let max_output = env::var("max_output_tokens")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(8_192);

        // Discover the upstream catalogue; on any failure fall back to the single
        // env-configured default model so existing installs never regress.
        let model_ids = match Self::discover_models() {
            Ok(ids) => ids,
            Err(e) => {
                log::warn(format!(
                    "/v1/models discovery failed, using env default: {e}"
                ));
                vec![default_model.clone()]
            }
        };

        let providers =
            Self::describe_providers(&model_ids, &default_model, context_window, max_output);
        let response = serde_json::json!({ "providers": providers });
        ipc::publish_json("llm.v1.response.describe", &response)?;
        Ok(response)
    }
}

impl OpenAICompatProvider {
    /// Query `GET {base_url}/v1/models` and return the discovered model ids.
    ///
    /// Returns `Ok(Vec)` with **at least one** id on success. Any failure
    /// (network error, non-2xx, unparseable body, empty `data`, server that
    /// does not implement `/v1/models`) returns `Err` so the caller falls back
    /// to the env default. Never panics; never blocks beyond the host HTTP
    /// timeout.
    fn discover_models() -> Result<Vec<String>, SysError> {
        let base_url = env::var("base_url").unwrap_or_else(|_| "https://api.openai.com".into());
        let url = format!("{}/v1/models", base_url.trim_end_matches('/'));

        let mut req = http::Request::get(&url);
        let api_key = env::var("api_key").unwrap_or_default();
        if !api_key.is_empty() {
            req = req.header("authorization", format!("Bearer {api_key}"));
        }

        let resp = http::send(&req)?;
        if !resp.is_success() {
            return Err(SysError::ApiError(format!(
                "/v1/models returned status {}",
                resp.status()
            )));
        }

        let list: ModelList = resp.json()?;
        Self::extract_model_ids(list)
    }

    /// Pure extraction + emptiness gate, split out so it is unit-testable
    /// without HTTP. Drops blank ids (empty or whitespace-only, which cannot be
    /// selected) and deduplicates colliding ids stably (preserving server
    /// order), guarding against hostile upstreams that repeat an id; errors if
    /// nothing usable remains so the caller falls back to the env default.
    fn extract_model_ids(list: ModelList) -> Result<Vec<String>, SysError> {
        let mut seen = std::collections::HashSet::new();
        let ids: Vec<String> = list
            .data
            .into_iter()
            .map(|m| m.id)
            .filter(|id| !id.trim().is_empty())
            .filter(|id| seen.insert(id.clone()))
            .collect();
        if ids.is_empty() {
            return Err(SysError::ApiError("/v1/models returned no models".into()));
        }
        Ok(ids)
    }

    /// Build one provider-entry per model id, with the env-default model emitted
    /// FIRST (`entry[0]`) so the registry can pre-select it positionally. Every
    /// entry shares the same `request_topic`/`stream_topic`. There is NO
    /// `"default"` field — the default is signalled by ORDER. All entries are
    /// plain `serde_json::Value`.
    ///
    /// Ordering rules:
    /// - If `default_model` appears in `model_ids`, its entry is `entry[0]` and
    ///   the remaining ids keep their discovered order after it.
    /// - If `default_model` is NOT in `model_ids` (the upstream catalogue does
    ///   not advertise the configured default), the discovered order is
    ///   preserved unchanged; `entry[0]` is simply the first discovered model.
    ///   The registry still auto-selects `entry[0]`; the operator's configured
    ///   `model` was not offered by the upstream, so the first servable model is
    ///   the best default.
    fn describe_providers(
        model_ids: &[String],
        default_model: &str,
        context_window: u64,
        max_output: u64,
    ) -> Vec<serde_json::Value> {
        // Stable partition: default model first (if present), then the rest in
        // their discovered order.
        let mut ordered: Vec<&String> = Vec::with_capacity(model_ids.len());
        ordered.extend(model_ids.iter().filter(|id| id.as_str() == default_model));
        ordered.extend(model_ids.iter().filter(|id| id.as_str() != default_model));

        ordered
            .iter()
            .map(|id| {
                serde_json::json!({
                    "id": id,
                    "description": format!("OpenAI-compatible model: {id}"),
                    "capabilities": ["text", "vision", "tools"],
                    "request_topic": "llm.v1.request.generate.openai-compat",
                    "stream_topic": "llm.v1.stream.openai-compat",
                    "context_window": context_window,
                    "max_output_tokens": max_output,
                })
            })
            .collect()
    }

    /// Build and send the HTTP request, then parse the SSE response.
    fn execute_request(
        request_id: Uuid,
        model: &str,
        messages: &[Message],
        tools: &[astrid_sdk::types::LlmToolDefinition],
        system: &str,
    ) -> Result<(), SysError> {
        let base_url = env::var("base_url").unwrap_or_else(|_| "https://api.openai.com".into());
        let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));

        let resolved_model = if model.is_empty() {
            env::var("model").unwrap_or_else(|_| "gpt-5.4".into())
        } else {
            model.to_string()
        };

        let mut api_messages: Vec<Value> = Vec::new();

        // System message goes first in the messages array.
        if !system.is_empty() {
            api_messages.push(serde_json::json!({
                "role": "system",
                "content": system,
            }));
        }

        for msg in messages {
            if msg.role != MessageRole::System {
                api_messages.push(Self::convert_message(msg));
            }
        }

        let mut request_body = serde_json::json!({
            "model": resolved_model,
            "messages": api_messages,
            "stream": true,
            "stream_options": { "include_usage": true },
        });

        // Apply default generation parameters from env (only if not already
        // specified in the request — env vars are defaults, not overrides).
        let has_max_tokens = request_body.get("max_tokens").is_some_and(|v| !v.is_null());
        if !has_max_tokens
            && let Ok(max_tokens) = env::var("max_output_tokens")
            && let Ok(n) = max_tokens.parse::<u64>()
            && n > 0
        {
            request_body["max_tokens"] = serde_json::json!(n);
        }
        let has_temp = request_body
            .get("temperature")
            .is_some_and(|v| !v.is_null());
        if !has_temp
            && let Ok(temp) = env::var("temperature")
            && let Ok(t) = temp.parse::<f64>()
        {
            request_body["temperature"] = serde_json::json!(t);
        }

        if !tools.is_empty() {
            let api_tools: Vec<Value> = tools
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "type": "function",
                        "function": {
                            "name": t.name,
                            "description": t.description,
                            "parameters": t.input_schema,
                        }
                    })
                })
                .collect();
            request_body["tools"] = Value::Array(api_tools);
        }

        let api_key = env::var("api_key").unwrap_or_default();
        if api_key.is_empty() {
            return Err(SysError::ApiError("api_key not configured".into()));
        }

        let req = http::Request::post(&url)
            .header("authorization", format!("Bearer {api_key}"))
            .json(&request_body)?;

        let stream = http::stream_start(&req)?;

        if stream.status() != 200 {
            // Drain the error body for the error message. Stream drops at
            // scope exit; no manual close required.
            let mut error_body = String::new();
            while let Some(chunk) = stream.read_chunk()? {
                error_body.push_str(&String::from_utf8_lossy(&chunk));
                if error_body.len() > 4096 {
                    error_body.truncate(4096);
                    break;
                }
            }
            return Err(SysError::ApiError(format!(
                "API error ({}): {error_body}",
                stream.status()
            )));
        }

        Self::parse_sse_stream_live(request_id, &stream)
        // `stream` drops here, releasing the kernel-side HTTP stream.
    }

    /// Stream SSE chunks in real-time, publishing IPC events as they arrive.
    fn parse_sse_stream_live(request_id: Uuid, stream: &http::HttpStream) -> Result<(), SysError> {
        let mut active_tools: Vec<(String, String)> = Vec::new();
        let mut line_buffer = String::new();

        while let Some(chunk) = stream.read_chunk()? {
            let chunk_str = String::from_utf8_lossy(&chunk);
            line_buffer.push_str(&chunk_str);

            if line_buffer.len() > MAX_LINE_BUFFER_SIZE {
                return Err(SysError::ApiError(
                    "SSE line buffer exceeded maximum size".into(),
                ));
            }

            // Process all complete lines in the buffer.
            while let Some(newline_pos) = line_buffer.find('\n') {
                let line = line_buffer[..newline_pos]
                    .trim_end_matches('\r')
                    .to_string();
                line_buffer = line_buffer[(newline_pos + 1)..].to_string();

                if line.is_empty() {
                    continue;
                }

                let Some(data) = line.strip_prefix("data: ") else {
                    continue;
                };

                if data == "[DONE]" {
                    Self::publish_stream(request_id, StreamEvent::Done)?;
                    return Ok(());
                }

                let Ok(chunk) = serde_json::from_str::<ChatCompletionChunk>(data) else {
                    continue;
                };

                Self::process_chunk(request_id, &chunk, &mut active_tools)?;
            }
        }

        Ok(())
    }

    /// Process a single parsed SSE chunk, emitting the appropriate stream events.
    fn process_chunk(
        request_id: Uuid,
        chunk: &ChatCompletionChunk,
        active_tools: &mut Vec<(String, String)>,
    ) -> Result<(), SysError> {
        // Handle usage (final chunk with empty choices).
        if let Some(usage) = &chunk.usage {
            Self::publish_stream(
                request_id,
                StreamEvent::Usage {
                    input_tokens: usage.prompt_tokens,
                    output_tokens: usage.completion_tokens,
                },
            )?;
        }

        let Some(choice) = chunk.choices.first() else {
            return Ok(());
        };

        // Handle text deltas.
        if let Some(ref text) = choice.delta.content
            && !text.is_empty()
        {
            Self::publish_stream(request_id, StreamEvent::TextDelta(text.clone()))?;
        }

        // Handle tool call deltas.
        if let Some(ref tool_calls) = choice.delta.tool_calls {
            for tc in tool_calls {
                // Grow the tracking vec if needed.
                while active_tools.len() <= tc.index {
                    active_tools.push((String::new(), String::new()));
                }

                if let Some(ref id) = tc.id {
                    active_tools[tc.index].0 = id.clone();
                }

                if let Some(ref func) = tc.function {
                    if let Some(ref name) = func.name {
                        active_tools[tc.index].1 = name.clone();
                        Self::publish_stream(
                            request_id,
                            StreamEvent::ToolCallStart {
                                id: active_tools[tc.index].0.clone(),
                                name: name.clone(),
                            },
                        )?;
                    }

                    if let Some(ref args) = func.arguments
                        && !args.is_empty()
                    {
                        Self::publish_stream(
                            request_id,
                            StreamEvent::ToolCallDelta {
                                id: active_tools[tc.index].0.clone(),
                                args_delta: args.clone(),
                            },
                        )?;
                    }
                }
            }
        }

        // Handle finish reason: emit ToolCallEnd for all active tool calls.
        if let Some(ref reason) = choice.finish_reason
            && reason == "tool_calls"
        {
            for (id, _name) in active_tools.iter() {
                if !id.is_empty() {
                    Self::publish_stream(request_id, StreamEvent::ToolCallEnd { id: id.clone() })?;
                }
            }
            active_tools.clear();
        }

        Ok(())
    }

    /// Publish a stream event to the event bus.
    fn publish_stream(request_id: Uuid, event: StreamEvent) -> Result<(), SysError> {
        ipc::publish_json(
            STREAM_TOPIC,
            &IpcPayload::LlmStreamEvent { request_id, event },
        )
    }

    /// Convert an Astrid `Message` to the OpenAI Chat Completions JSON format.
    fn convert_message(message: &Message) -> Value {
        match &message.content {
            MessageContent::Text(text) => {
                serde_json::json!({
                    "role": Self::role_str(message.role),
                    "content": text,
                })
            }
            MessageContent::ToolCalls(calls) => {
                let tool_calls: Vec<Value> = calls
                    .iter()
                    .map(|c| {
                        serde_json::json!({
                            "id": c.id,
                            "type": "function",
                            "function": {
                                "name": c.name,
                                "arguments": if c.arguments.is_string() {
                                    c.arguments.clone()
                                } else {
                                    Value::String(c.arguments.to_string())
                                },
                            }
                        })
                    })
                    .collect();

                serde_json::json!({
                    "role": "assistant",
                    "content": null,
                    "tool_calls": tool_calls,
                })
            }
            MessageContent::ToolResult(result) => {
                serde_json::json!({
                    "role": "tool",
                    "tool_call_id": result.call_id,
                    "content": result.content,
                })
            }
            MessageContent::MultiPart(parts) => {
                let content: Vec<Value> = parts
                    .iter()
                    .map(|p| match p {
                        astrid_sdk::types::ContentPart::Text { text } => {
                            serde_json::json!({"type": "text", "text": text})
                        }
                        astrid_sdk::types::ContentPart::Image { media_type, data } => {
                            serde_json::json!({
                                "type": "image_url",
                                "image_url": {
                                    "url": format!("data:{media_type};base64,{data}"),
                                }
                            })
                        }
                    })
                    .collect();

                serde_json::json!({
                    "role": Self::role_str(message.role),
                    "content": content,
                })
            }
        }
    }

    /// Map Astrid `MessageRole` to OpenAI role string.
    fn role_str(role: MessageRole) -> &'static str {
        match role {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelList, OpenAICompatProvider};

    const REQUEST_TOPIC: &str = "llm.v1.request.generate.openai-compat";

    /// Helper: collect the `id` field of every provider entry, in order.
    fn ids(entries: &[serde_json::Value]) -> Vec<String> {
        entries
            .iter()
            .map(|e| e["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn parse_models_body_yields_one_entry_per_id() {
        // Representative `/v1/models` body with vendor-specific extra fields and
        // an ollama-style colon id alongside a real OpenAI-style id.
        let body = r#"{
            "object": "list",
            "data": [
                { "id": "gpt-5.4", "object": "model", "owned_by": "openai" },
                { "id": "llama3.3:70b", "object": "model", "owned_by": "library" },
                { "id": "mixtral-8x7b", "object": "model", "created": 1700000000 }
            ]
        }"#;
        let list: ModelList = serde_json::from_str(body).expect("parse model list");
        assert_eq!(list.data.len(), 3);
        // The colon in the ollama-style id survives deserialization verbatim.
        assert_eq!(list.data[1].id, "llama3.3:70b");

        let model_ids = OpenAICompatProvider::extract_model_ids(list).expect("non-empty");
        assert_eq!(model_ids, vec!["gpt-5.4", "llama3.3:70b", "mixtral-8x7b"]);

        let entries =
            OpenAICompatProvider::describe_providers(&model_ids, "gpt-5.4", 128_000, 8_192);
        assert_eq!(entries.len(), 3);
        for entry in &entries {
            assert_eq!(entry["request_topic"].as_str().unwrap(), REQUEST_TOPIC);
        }
        // `id` equals the source model id end-to-end (colon preserved).
        assert_eq!(
            ids(&entries),
            vec!["gpt-5.4", "llama3.3:70b", "mixtral-8x7b"]
        );
    }

    #[test]
    fn describe_emits_env_model_first() {
        // Default model is present but NOT first in discovered order.
        let model_ids = vec![
            "first-discovered".to_string(),
            "the-default".to_string(),
            "last-discovered".to_string(),
        ];
        let entries =
            OpenAICompatProvider::describe_providers(&model_ids, "the-default", 128_000, 8_192);

        // entry[0] is the env default; the rest keep their discovered order.
        assert_eq!(entries[0]["id"].as_str().unwrap(), "the-default");
        assert_eq!(
            ids(&entries),
            vec!["the-default", "first-discovered", "last-discovered"]
        );

        // No entry carries a "default" key — ordering is the only signal.
        for entry in &entries {
            assert!(entry.get("default").is_none());
        }
    }

    #[test]
    fn describe_preserves_order_when_default_absent() {
        // Default model is NOT in the discovered list.
        let model_ids = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let entries =
            OpenAICompatProvider::describe_providers(&model_ids, "not-present", 128_000, 8_192);

        // Discovered order preserved unchanged; nothing dropped.
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0]["id"].as_str().unwrap(), "alpha");
        assert_eq!(ids(&entries), vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn empty_data_is_discovery_error() {
        // Empty `data` array funnels to the fallback Err arm.
        let empty: ModelList = serde_json::from_str(r#"{ "data": [] }"#).expect("parse");
        assert!(OpenAICompatProvider::extract_model_ids(empty).is_err());

        // A sole entry with an empty `id` is dropped, leaving nothing usable.
        let blank: ModelList =
            serde_json::from_str(r#"{ "data": [ { "id": "" } ] }"#).expect("parse");
        assert!(OpenAICompatProvider::extract_model_ids(blank).is_err());

        // A sole entry with a whitespace-only `id` is likewise dropped: it
        // cannot be selected, so it funnels to the same fallback Err arm.
        let whitespace: ModelList =
            serde_json::from_str(r#"{ "data": [ { "id": "   " } ] }"#).expect("parse");
        assert!(OpenAICompatProvider::extract_model_ids(whitespace).is_err());
    }

    #[test]
    fn duplicate_ids_are_deduplicated_preserving_order() {
        // A buggy/hostile upstream that repeats an id must not yield two
        // provider entries with the same id. Dedup is stable on server order.
        let dup: ModelList =
            serde_json::from_str(r#"{ "data": [ { "id": "gpt-5.4" }, { "id": "gpt-5.4" } ] }"#)
                .expect("parse");
        let ids = OpenAICompatProvider::extract_model_ids(dup).expect("one survivor");
        assert_eq!(ids, vec!["gpt-5.4"]);
        assert_eq!(ids.len(), 1);

        // Non-adjacent collisions collapse too, and the first occurrence wins
        // (server order preserved for the survivors).
        let scattered: ModelList = serde_json::from_str(
            r#"{ "data": [ { "id": "a" }, { "id": "b" }, { "id": "a" }, { "id": "c" } ] }"#,
        )
        .expect("parse");
        let ids = OpenAICompatProvider::extract_model_ids(scattered).expect("survivors");
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn unparseable_body_is_discovery_error() {
        // A non-JSON / HTML body (e.g. a 404 page) fails to deserialize, which
        // is the Err that triggers the env fallback in `discover_models`.
        let result: Result<ModelList, _> = serde_json::from_str("<html>404</html>");
        assert!(result.is_err());
    }

    #[test]
    fn fallback_advertisement_matches_single_model_shape() {
        // The discovery-failure path advertises a single env-model entry.
        let entries = OpenAICompatProvider::describe_providers(
            &["gpt-5.4".to_string()],
            "gpt-5.4",
            128_000,
            8_192,
        );

        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry["id"].as_str().unwrap(), "gpt-5.4");
        assert_eq!(entry["request_topic"].as_str().unwrap(), REQUEST_TOPIC);
        // Shape-stable, positionally-default, no "default" key.
        assert!(entry.get("default").is_none());
    }
}
