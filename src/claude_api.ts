import type { Env } from "./common";
import {
  jsonError,
  jsonResponse,
  joinPathPrefix,
  logDebug,
  normalizeAuthValue,
  normalizeBaseUrl,
  normalizeMessageContent,
  previewString,
  safeJsonStringifyForLog,
  sseHeaders,
} from "./common";

function encodeSseEvent(event: string, dataStr: string): string {
  return `event: ${event}\ndata: ${dataStr}\n\n`;
}

function openaiFinishReasonToClaude(stopReason: unknown): string {
  if (stopReason === "tool_calls") return "tool_use";
  if (stopReason === "length") return "max_tokens";
  if (stopReason === "stop") return "end_turn";
  return "end_turn";
}

function openaiToolChoiceFromClaude(toolChoice: any): any {
  if (!toolChoice || typeof toolChoice !== "object") return undefined;
  const t = toolChoice.type;
  if (t === "auto") return "auto";
  if (t === "any") return "required";
  if (t === "tool") {
    const name = typeof toolChoice.name === "string" ? toolChoice.name.trim() : "";
    if (!name) return undefined;
    return { type: "function", function: { name } };
  }
  return undefined;
}

function openaiToolsFromClaude(tools: any): any[] {
  const list = Array.isArray(tools) ? tools : [];
  const out: any[] = [];
  for (const t of list) {
    if (!t || typeof t !== "object") continue;
    const name = typeof t.name === "string" ? t.name.trim() : "";
    if (!name) continue;
    const description = typeof t.description === "string" ? t.description : "";
    const parameters = t.input_schema && typeof t.input_schema === "object" ? t.input_schema : { type: "object", properties: {} };
    out.push({ type: "function", function: { name, description, parameters } });
  }
  return out;
}

function claudeSystemToOpenaiMessages(system: any): any[] {
  if (system == null) return [];
  if (typeof system === "string") {
    const txt = system.trim();
    return txt ? [{ role: "system", content: txt }] : [];
  }
  if (Array.isArray(system)) {
    const parts: string[] = [];
    for (const b of system) {
      if (b && typeof b === "object" && b.type === "text" && typeof b.text === "string" && b.text.trim()) {
        parts.push(b.text.trim());
      }
    }
    const txt = parts.join("\n\n").trim();
    return txt ? [{ role: "system", content: txt }] : [];
  }
  return [];
}

function claudeContentBlocksToOpenaiParts(blocks: any): any {
  if (typeof blocks === "string") return blocks;
  const list = Array.isArray(blocks) ? blocks : [];
  const parts: any[] = [];
  for (const b of list) {
    if (!b || typeof b !== "object") continue;
    if (b.type === "text") {
      if (typeof b.text === "string") parts.push({ type: "text", text: b.text });
      continue;
    }
    if (b.type === "image") {
      const src = b.source;
      if (!src || typeof src !== "object") continue;
      if (src.type === "base64" && typeof src.data === "string" && src.data.trim()) {
        const mt = typeof src.media_type === "string" && src.media_type.trim() ? src.media_type.trim() : "image/png";
        const url = `data:${mt};base64,${src.data.trim()}`;
        parts.push({ type: "image_url", image_url: { url } });
      }
      continue;
    }
  }
  if (!parts.length) return "";
  return parts;
}

function claudeMessagesToOpenaiChatMessages(messages: any): any[] {
  const out: any[] = [];
  for (const msg of Array.isArray(messages) ? messages : []) {
    if (!msg || typeof msg !== "object") continue;
    const role = msg.role === "assistant" ? "assistant" : "user";
    const content = msg.content;

    // Convert tool results (Anthropic sends them inside user messages).
    if (Array.isArray(content)) {
      const toolResultBlocks = content.filter((b) => b && typeof b === "object" && b.type === "tool_result");
      if (toolResultBlocks.length) {
        for (const tr of toolResultBlocks) {
          const id = typeof tr.tool_use_id === "string" ? tr.tool_use_id.trim() : "";
          if (!id) continue;
          const output = normalizeMessageContent(tr.content);
          out.push({ role: "tool", tool_call_id: id, content: output ?? "" });
        }
        const nonTool = content.filter((b) => !(b && typeof b === "object" && b.type === "tool_result"));
        if (!nonTool.length) continue;
        out.push({ role: "user", content: claudeContentBlocksToOpenaiParts(nonTool) });
        continue;
      }
    }

    // Convert assistant tool_use blocks into OpenAI tool_calls so downstream (Responses) can
    // validate and associate subsequent tool results. This is especially important when
    // `previous_response_id` is disabled and we must send full history.
    if (role === "assistant" && Array.isArray(content)) {
      const toolCalls = [];
      const nonToolBlocks = [];
      for (const b of content) {
        if (!b || typeof b !== "object") continue;
        if (b.type === "tool_use") {
          const id = typeof b.id === "string" && b.id.trim() ? b.id.trim() : `call_${crypto.randomUUID().replace(/-/g, "")}`;
          const name = typeof b.name === "string" ? b.name : "";
          let args = "{}";
          try {
            args = JSON.stringify(b.input ?? {});
          } catch {}
          toolCalls.push({ id, type: "function", function: { name, arguments: args } });
          continue;
        }
        // tool_result should not appear in assistant messages, but if it does, ignore it.
        if (b.type === "tool_result") continue;
        nonToolBlocks.push(b);
      }
      const msgOut: Record<string, any> = { role: "assistant", content: claudeContentBlocksToOpenaiParts(nonToolBlocks) };
      if (toolCalls.length) msgOut.tool_calls = toolCalls;
      out.push(msgOut);
      continue;
    }

    out.push({ role, content: claudeContentBlocksToOpenaiParts(content) });
  }
  return out;
}

function buildClaudeCountTokensUrls(rawBase: unknown, messagesPathRaw: unknown): string[] {
  const value = typeof rawBase === "string" ? rawBase.trim() : "";
  if (!value) return [];

  const configuredMessagesPath = typeof messagesPathRaw === "string" ? messagesPathRaw.trim() : "";
  const parts = value.split(",").map((p) => p.trim()).filter(Boolean);

  const out: string[] = [];
  const pushUrl = (urlStr: unknown) => {
    if (!urlStr || typeof urlStr !== "string") return;
    if (!out.includes(urlStr)) out.push(urlStr);
  };

  const inferMessagesPath = (basePath: string) => {
    const p = (basePath || "").replace(/\/+$/, "");
    if (p.endsWith("/v1")) return "/messages";
    return "/v1/messages";
  };

  for (const p of parts) {
    try {
      const normalized = normalizeBaseUrl(p);
      const u0 = new URL(normalized);
      const basePath = (u0.pathname || "").replace(/\/+$/, "") || "/";

      const messagesPath = configuredMessagesPath
        ? configuredMessagesPath.startsWith("/")
          ? configuredMessagesPath
          : `/${configuredMessagesPath}`
        : inferMessagesPath(basePath);
      const countTokensPath = joinPathPrefix(messagesPath, "/count_tokens");

      const u = new URL(normalized);
      u.pathname = countTokensPath;
      u.search = "";
      u.hash = "";
      pushUrl(u.toString());
    } catch {
      // ignore invalid urls
    }
  }

  return out;
}

function estimateTokensFromText(text: unknown): number {
  const s = typeof text === "string" ? text : text == null ? "" : String(text);
  if (!s) return 0;
  // Rough heuristic: ~4 bytes per token for English; CJK tends to be denser but this is "good enough" for budgeting.
  const bytes = new TextEncoder().encode(s).length;
  return Math.max(1, Math.ceil(bytes / 4));
}

function estimateClaudeInputTokens(reqJson: any): number {
  let tokens = 0;

  // System
  if (typeof reqJson?.system === "string") tokens += estimateTokensFromText(reqJson.system) + 6;
  if (Array.isArray(reqJson?.system)) {
    for (const b of reqJson.system) {
      if (b && typeof b === "object" && b.type === "text" && typeof b.text === "string") tokens += estimateTokensFromText(b.text);
    }
    tokens += 6;
  }

  // Messages
  const messages = Array.isArray(reqJson?.messages) ? reqJson.messages : [];
  for (const m of messages) {
    if (!m || typeof m !== "object") continue;
    tokens += 4; // per-message overhead (very rough)
    tokens += estimateTokensFromText(m.role || "");

    const content = m.content;
    if (typeof content === "string") {
      tokens += estimateTokensFromText(content);
      continue;
    }
    const blocks = Array.isArray(content) ? content : [];
    for (const b of blocks) {
      if (!b || typeof b !== "object") continue;
      const t = b.type;
      tokens += estimateTokensFromText(t || "");
      if (t === "text") {
        tokens += estimateTokensFromText(b.text || "");
      } else if (t === "tool_use") {
        tokens += estimateTokensFromText(b.name || "");
        try {
          tokens += estimateTokensFromText(JSON.stringify(b.input ?? {}));
        } catch {}
      } else if (t === "tool_result") {
        tokens += estimateTokensFromText(b.tool_use_id || "");
        tokens += estimateTokensFromText(normalizeMessageContent(b.content));
      } else if (t === "image") {
        // Avoid counting huge base64 payloads; treat as a fixed budget.
        tokens += 1500;
      }
    }
  }

  // Tools: schemas can be large; budget them in.
  const tools = Array.isArray(reqJson?.tools) ? reqJson.tools : [];
  for (const t of tools) {
    if (!t || typeof t !== "object") continue;
    tokens += 6;
    tokens += estimateTokensFromText(t.name || "");
    tokens += estimateTokensFromText(t.description || "");
    try {
      tokens += estimateTokensFromText(JSON.stringify(t.input_schema ?? {}));
    } catch {}
  }

  // Tool choice
  if (reqJson?.tool_choice) {
    try {
      tokens += 4 + estimateTokensFromText(JSON.stringify(reqJson.tool_choice));
    } catch {
      tokens += 4;
    }
  }

  return Math.max(1, Math.floor(tokens));
}

export function claudeMessagesRequestToOpenaiChat(
  reqJson: any,
): { ok: true; status: 200; req: Record<string, any> } | { ok: false; status: number; error: any } {
  const model = typeof reqJson?.model === "string" ? reqJson.model.trim() : "";
  if (!model) return { ok: false, status: 400, error: jsonError("Missing required field: model") };

  const maxTokens = Number.isInteger(reqJson?.max_tokens) ? reqJson.max_tokens : undefined;
  const stream = Boolean(reqJson?.stream);
  const temperature = typeof reqJson?.temperature === "number" ? reqJson.temperature : undefined;
  const topP = typeof reqJson?.top_p === "number" ? reqJson.top_p : undefined;

  const messages = [
    ...claudeSystemToOpenaiMessages(reqJson?.system),
    ...claudeMessagesToOpenaiChatMessages(reqJson?.messages),
  ];

  const out: Record<string, any> = { model, messages, stream };
  if (Number.isInteger(maxTokens)) out.max_tokens = maxTokens;
  if (temperature != null) out.temperature = temperature;
  if (topP != null) out.top_p = topP;

  const tools = openaiToolsFromClaude(reqJson?.tools);
  if (tools.length) out.tools = tools;

  const toolChoice = openaiToolChoiceFromClaude(reqJson?.tool_choice);
  if (toolChoice != null) out.tool_choice = toolChoice;

  // Pass through a stable "user" for session caching if the client provides metadata.
  const metadataUser =
    typeof reqJson?.metadata?.user_id === "string"
      ? reqJson.metadata.user_id.trim()
      : typeof reqJson?.metadata?.user === "string"
        ? reqJson.metadata.user.trim()
        : "";
  if (metadataUser) out.user = metadataUser;

  return { ok: true, status: 200, req: out };
}

export function openaiChatResponseToClaudeMessage(openaiJson: any): Record<string, any> {
  const id = `msg_${crypto.randomUUID().replace(/-/g, "")}`;
  const model = typeof openaiJson?.model === "string" ? openaiJson.model : "unknown";
  const choice = Array.isArray(openaiJson?.choices) ? openaiJson.choices[0] : null;
  const finish = choice?.finish_reason || "stop";
  const stopReason = openaiFinishReasonToClaude(finish);

  const msg = choice?.message && typeof choice.message === "object" ? choice.message : {};
  const text = typeof msg.content === "string" ? msg.content : msg.content == null ? "" : String(msg.content);
  const content = [];
  if (text) content.push({ type: "text", text });

  const toolCalls = Array.isArray(msg.tool_calls) ? msg.tool_calls : [];
  for (const tc of toolCalls) {
    if (!tc || typeof tc !== "object") continue;
    const tcId = typeof tc.id === "string" ? tc.id : `toolu_${crypto.randomUUID().replace(/-/g, "")}`;
    const name = tc.function && typeof tc.function === "object" && typeof tc.function.name === "string" ? tc.function.name : "";
    let input = {};
    const args = tc.function && typeof tc.function === "object" ? tc.function.arguments : undefined;
    try {
      if (typeof args === "string" && args.trim()) input = JSON.parse(args);
    } catch {}
    content.push({ type: "tool_use", id: tcId, name, input });
  }

  const usage = openaiJson?.usage && typeof openaiJson.usage === "object" ? openaiJson.usage : null;
  const inputTokens = usage && Number.isInteger(usage.prompt_tokens) ? usage.prompt_tokens : 0;
  const outputTokens = usage && Number.isInteger(usage.completion_tokens) ? usage.completion_tokens : 0;

  return {
    id,
    type: "message",
    role: "assistant",
    model,
    content,
    stop_reason: stopReason,
    stop_sequence: null,
    usage: { input_tokens: inputTokens, output_tokens: outputTokens },
  };
}

export async function handleClaudeCountTokens({
  request,
  env,
  reqJson,
  debug,
  reqId,
}: {
  request: Request;
  env: Env;
  reqJson: any;
  debug: boolean;
  reqId: string;
}): Promise<Response> {
  const model = typeof reqJson?.model === "string" ? reqJson.model.trim() : "";
  if (!model) return jsonResponse(400, jsonError("Missing required field: model"));
  if (debug) logDebug(debug, reqId, "claude count_tokens request", { model });

  // If the requested model is a Claude model and we have a Claude upstream, proxy to its count_tokens for better accuracy.
  const claudeBase = normalizeAuthValue(env?.CLAUDE_BASE_URL);
  const claudeKey = normalizeAuthValue(env?.CLAUDE_API_KEY);
  const shouldProxy = Boolean(claudeBase && claudeKey) && model.toLowerCase().includes("claude");

  if (shouldProxy) {
    const urls = buildClaudeCountTokensUrls(claudeBase, env?.CLAUDE_MESSAGES_PATH);
    if (debug) logDebug(debug, reqId, "claude count_tokens upstream urls", { urls });

    const headers = {
      "content-type": "application/json",
      authorization: `Bearer ${claudeKey}`,
      "x-api-key": claudeKey,
      "anthropic-version": request.headers.get("anthropic-version") || "2023-06-01",
    };
    if (debug) logDebug(debug, reqId, "claude count_tokens upstream headers", { headers: { ...headers, authorization: "Bearer …" } });

    let lastText = "";
    for (const url of urls) {
      try {
        const t0 = Date.now();
        const resp = await fetch(url, { method: "POST", headers, body: JSON.stringify(reqJson) });
        const elapsedMs = Date.now() - t0;
        if (debug) logDebug(debug, reqId, "claude count_tokens upstream response", { url, status: resp.status, ok: resp.ok, elapsedMs });
        if (!resp.ok) {
          lastText = await resp.text().catch(() => "");
          continue;
        }
        const j: any = await resp.json().catch(() => null);
        const inputTokens =
          j && typeof j === "object" && Number.isInteger(j.input_tokens)
            ? j.input_tokens
            : j && typeof j === "object" && Number.isInteger(j?.usage?.input_tokens)
              ? j.usage.input_tokens
              : null;
        if (Number.isInteger(inputTokens)) {
          if (debug) logDebug(debug, reqId, "claude count_tokens result", { mode: "proxy", model, input_tokens: inputTokens });
          return jsonResponse(200, { input_tokens: inputTokens });
        }
        // If upstream returns an unexpected shape, fallback to estimation.
        break;
      } catch (err) {
        lastText = err instanceof Error ? err.message : String(err ?? "fetch failed");
        continue;
      }
    }

    if (debug && lastText) logDebug(debug, reqId, "claude count_tokens proxy failed; fallback to estimate", { error: previewString(lastText, 400) });
  }

  // Fallback: approximate token count so claude-cli can budget & proceed.
  const inputTokens = estimateClaudeInputTokens(reqJson);
  if (debug) logDebug(debug, reqId, "claude count_tokens result", { mode: "estimate", model, input_tokens: inputTokens });
  return jsonResponse(200, { input_tokens: inputTokens });
}

export async function openaiStreamToClaudeMessagesSse(
  openaiResp: Response,
  { reqModel, debug, reqId }: { reqModel: string; debug: boolean; reqId: string },
): Promise<
  | { ok: true; status: 200; resp: Response }
  | { ok: false; status: number; error: { error: { message: string; type: string; code: string } } }
> {
  const ct = (openaiResp?.headers?.get("content-type") || "").toLowerCase();
  if (!ct.includes("text/event-stream") || !openaiResp?.body) {
    return { ok: false, status: 502, error: jsonError("Upstream did not return an event stream", "bad_gateway") };
  }

  const msgId = `msg_${crypto.randomUUID().replace(/-/g, "")}`;
  let outModel = reqModel || "unknown";

  const upstreamBody = openaiResp.body;
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  const started = { text: false, tool: new Set<string>() };
  let finished = false;
  let finishReason = "end_turn";

  const startMessage = async () => {
    const payload = {
      type: "message_start",
      message: {
        id: msgId,
        type: "message",
        role: "assistant",
        model: outModel,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 0, output_tokens: 0 },
      },
    };
    await writer.write(encoder.encode(encodeSseEvent("message_start", JSON.stringify(payload))));
  };

  const startTextBlock = async () => {
    if (started.text) return;
    started.text = true;
    await writer.write(
      encoder.encode(
        encodeSseEvent(
          "content_block_start",
          JSON.stringify({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }),
        ),
      ),
    );
  };

  const stopTextBlock = async () => {
    if (!started.text) return;
    await writer.write(encoder.encode(encodeSseEvent("content_block_stop", JSON.stringify({ type: "content_block_stop", index: 0 }))));
  };

  const startToolBlock = async (index: number, id: string, name: string): Promise<void> => {
    const key = String(index);
    if (started.tool.has(key)) return;
    started.tool.add(key);
    await writer.write(
      encoder.encode(
        encodeSseEvent(
          "content_block_start",
          JSON.stringify({ type: "content_block_start", index, content_block: { type: "tool_use", id, name, input: {} } }),
        ),
      ),
    );
  };

  const stopToolBlocks = async (): Promise<void> => {
    for (const key of Array.from(started.tool.values())) {
      const idx = parseInt(key, 10);
      if (!Number.isInteger(idx)) continue;
      await writer.write(encoder.encode(encodeSseEvent("content_block_stop", JSON.stringify({ type: "content_block_stop", index: idx }))));
    }
  };

  const stopMessage = async (): Promise<void> => {
    const delta = { type: "message_delta", delta: { stop_reason: finishReason, stop_sequence: null }, usage: { output_tokens: 0 } };
    await writer.write(encoder.encode(encodeSseEvent("message_delta", JSON.stringify(delta))));
    await writer.write(encoder.encode(encodeSseEvent("message_stop", JSON.stringify({ type: "message_stop" }))));
  };

  (async (): Promise<void> => {
    const streamStartedAt = Date.now();
    try {
      await startMessage();
      const reader = upstreamBody.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split("\n");
        buf = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          const data = line.slice(5).trim();
          if (!data) continue;
          if (data === "[DONE]") {
            finished = true;
            break;
          }
          let chunk: any;
          try {
            chunk = JSON.parse(data);
          } catch {
            continue;
          }
          if (chunk && typeof chunk.model === "string" && chunk.model) outModel = chunk.model;
          const choice = Array.isArray(chunk?.choices) ? chunk.choices[0] : null;
          const delta = choice?.delta && typeof choice.delta === "object" ? choice.delta : null;
          const finish = choice?.finish_reason;
          if (typeof finish === "string" && finish) finishReason = openaiFinishReasonToClaude(finish);

          if (delta && typeof delta.content === "string" && delta.content) {
            await startTextBlock();
            const evt = {
              type: "content_block_delta",
              index: 0,
              delta: { type: "text_delta", text: delta.content },
            };
            await writer.write(encoder.encode(encodeSseEvent("content_block_delta", JSON.stringify(evt))));
          }

          const toolCalls = Array.isArray(delta?.tool_calls) ? delta.tool_calls : [];
          for (const tc of toolCalls) {
            if (!tc || typeof tc !== "object") continue;
            const openaiIdx = Number.isInteger(tc.index) ? tc.index : 0;
            const idx = 1 + openaiIdx;
            const tcId = typeof tc.id === "string" && tc.id.trim() ? tc.id.trim() : `toolu_${crypto.randomUUID().replace(/-/g, "")}`;
            const name = tc.function && typeof tc.function === "object" && typeof tc.function.name === "string" ? tc.function.name : "";
            await startToolBlock(idx, tcId, name);
            const argsDelta = tc.function && typeof tc.function === "object" && typeof tc.function.arguments === "string" ? tc.function.arguments : "";
            if (argsDelta) {
              const evt = {
                type: "content_block_delta",
                index: idx,
                delta: { type: "input_json_delta", partial_json: argsDelta },
              };
              await writer.write(encoder.encode(encodeSseEvent("content_block_delta", JSON.stringify(evt))));
            }
          }
        }
        if (finished) break;
      }

      await stopTextBlock();
      await stopToolBlocks();
      await stopMessage();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err ?? "stream error");
      if (debug) logDebug(debug, reqId, "claude api stream transform error", { error: message });
    } finally {
      if (debug) {
        logDebug(debug, reqId, "claude api stream transform done", { elapsedMs: Date.now() - streamStartedAt, finishReason });
      }
      try {
        await writer.close();
      } catch {}
    }
  })();

  return { ok: true, status: 200, resp: new Response(readable, { status: 200, headers: sseHeaders() }) };
}
