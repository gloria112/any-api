export type Env = Record<string, string | undefined>;

export function jsonResponse(status: number, obj: unknown, extraHeaders: Record<string, unknown> | undefined = undefined): Response {
  const headers: Record<string, string> = {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  };
  if (extraHeaders && typeof extraHeaders === "object") {
    for (const [k, v] of Object.entries(extraHeaders)) {
      if (v == null) continue;
      headers[k] = String(v);
    }
  }
  return new Response(JSON.stringify(obj), { status, headers });
}

export function jsonError(message: string, code: string = "bad_request"): { error: { message: string; type: string; code: string } } {
  return { error: { message, type: "invalid_request_error", code } };
}

export function parseBoolEnv(value: unknown): boolean {
  const v = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (!v) return false;
  return v === "1" || v === "true" || v === "yes" || v === "y" || v === "on";
}

export function isDebugEnabled(env: Env): boolean {
  return parseBoolEnv(typeof env?.ANY_API_DEBUG === "string" ? env.ANY_API_DEBUG : "");
}

export function maskSecret(value: unknown): string {
  const v = typeof value === "string" ? value : value == null ? "" : String(value);
  if (!v) return "";
  const s = v.trim();
  if (!s) return "";
  if (s.length <= 8) return `${"*".repeat(s.length)} (len=${s.length})`;
  return `${s.slice(0, 4)}…${s.slice(-4)} (len=${s.length})`;
}

export function previewString(value: unknown, maxLen: number = 1200): string {
  const v = typeof value === "string" ? value : value == null ? "" : String(value);
  if (!v) return "";
  if (v.length <= maxLen) return v;
  return `${v.slice(0, maxLen)}…(truncated,len=${v.length})`;
}

export function redactHeadersForLog(headers: unknown): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const h = headers && typeof headers === "object" ? (headers as Record<string, unknown>) : {};
  for (const [k, v] of Object.entries(h)) {
    const key = String(k || "");
    const lower = key.toLowerCase();
    if (lower === "authorization" || lower.includes("api-key") || lower.includes("token") || lower.includes("secret")) {
      out[key] = maskSecret(v);
      continue;
    }
    out[key] = typeof v === "string" ? previewString(v, 200) : v;
  }
  return out;
}

export function safeJsonStringifyForLog(value: unknown, maxStringLen: number = 800): string {
  const seen = new WeakSet();
  const isSensitiveKey = (keyLower: string): boolean =>
    keyLower === "authorization" ||
    keyLower.includes("api_key") ||
    keyLower.endsWith("api-key") ||
    keyLower.endsWith("_key") ||
    keyLower.endsWith("key") ||
    keyLower.includes("token") ||
    keyLower.includes("password") ||
    keyLower.includes("passwd") ||
    keyLower.includes("secret");

  return JSON.stringify(value, (key: string, v: unknown) => {
    if (v && typeof v === "object") {
      if (seen.has(v as object)) return "[Circular]";
      seen.add(v as object);
      return v;
    }
    if (typeof v === "string") {
      const keyLower = String(key || "").toLowerCase();
      if (isSensitiveKey(keyLower)) return maskSecret(v);
      return previewString(v, maxStringLen);
    }
    return v;
  });
}

export function logDebug(enabled: boolean, reqId: string, label: string, data: unknown = undefined): void {
  if (!enabled) return;
  const prefix = reqId ? `[any-api][${reqId}]` : "[any-api]";
  if (data === undefined) {
    console.log(`${prefix} ${label}`);
  } else {
    console.log(`${prefix} ${label}`, data);
  }
}

export function normalizeAuthValue(raw: unknown): string {
  if (typeof raw !== "string") return "";
  let value = raw.trim();
  if (!value) return "";

  // Strip accidental surrounding quotes (common when copy/pasting secrets).
  for (let i = 0; i < 2; i++) {
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1).trim();
      continue;
    }
    break;
  }

  // Strip accidental "Bearer " prefix in stored secrets.
  if (value.toLowerCase().startsWith("bearer ")) value = value.slice(7).trim();

  return value;
}

export function getWorkerAuthKeys(env: Env): string[] {
  const keys = new Set<string>();

  const single = normalizeAuthValue(env?.WORKER_AUTH_KEY);
  if (single) keys.add(single);

  const multiRaw = typeof env?.WORKER_AUTH_KEYS === "string" ? env.WORKER_AUTH_KEYS : "";
  for (const part of String(multiRaw || "").split(",")) {
    const k = normalizeAuthValue(part);
    if (k) keys.add(k);
  }

  return Array.from(keys);
}

export function bearerToken(headerValue: string | null): string | null {
  if (!headerValue || typeof headerValue !== "string") return null;
  const value = headerValue.trim();
  if (value.toLowerCase().startsWith("bearer ")) return value.slice(7).trim();
  return null;
}

export function normalizeBaseUrl(raw: string): string {
  const base = (raw || "").trim();
  if (!base) return base;
  const lower = base.toLowerCase();
  if (lower === "http" || lower === "http:" || lower === "https" || lower === "https:") return "";
  if (base.startsWith("http://") || base.startsWith("https://")) return base;
  return `https://${base}`;
}

export function joinPathPrefix(prefixPath: string, suffixPath: string): string {
  const p = (prefixPath || "").replace(/\/+$/, "");
  const s0 = (suffixPath || "").trim();
  const s = s0.startsWith("/") ? s0 : `/${s0}`;
  if (!p || p === "/") return s;
  return `${p}${s}`;
}

export function applyTemperatureTopPFromRequest(reqJson: unknown, target: Record<string, unknown>): void {
  if (!reqJson || typeof reqJson !== "object") return;
  if (!target || typeof target !== "object") return;

  const req = reqJson as Record<string, unknown>;

  // Some relays reject requests that specify both temperature and top_p.
  // Prefer temperature when both are present (some clients send both).
  const hasTemp = Object.prototype.hasOwnProperty.call(reqJson, "temperature");
  const hasTopP = Object.prototype.hasOwnProperty.call(reqJson, "top_p");

  if (hasTemp && hasTopP) {
    const temp = req.temperature;
    const topP = req.top_p;
    if ((temp == null || temp === "") && topP != null && topP !== "") {
      target.top_p = topP;
    } else {
      target.temperature = temp;
    }
    return;
  }

  if (hasTemp) target.temperature = req.temperature;
  if (hasTopP) target.top_p = req.top_p;
}

export function normalizeMessageContent(content: unknown): string {
  if (content == null) return "";
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts = [];
    for (const item of content) {
      if (typeof item === "string") {
        parts.push(item);
        continue;
      }
      if (item && typeof item === "object") {
        const t = item.type;
        if (t === "text" || t === "input_text" || t === "output_text") {
          if (typeof item.text === "string") parts.push(item.text);
        } else if (typeof item.text === "string") {
          parts.push(item.text);
        }
      }
    }
    return parts.join("");
  }
  if (content && typeof content === "object" && !Array.isArray(content)) {
    const obj = content as Record<string, unknown>;
    if (typeof obj.text === "string") return obj.text;
  }
  return String(content);
}

export interface NormalizedToolCall {
  call_id: string;
  name: string;
  arguments: string;
  thought_signature?: string;
  thought?: string;
}

export function normalizeToolCallsFromChatMessage(msg: unknown): NormalizedToolCall[] {
  const out: NormalizedToolCall[] = [];
  if (!msg || typeof msg !== "object") return out;

  const pushToolCall = (
    id: unknown,
    name: unknown,
    args: unknown,
    thoughtSignature: unknown,
    thought: unknown,
  ) => {
    const callId = typeof id === "string" && id.trim() ? id.trim() : `call_${crypto.randomUUID().replace(/-/g, "")}`;
    const toolName = typeof name === "string" && name.trim() ? name.trim() : "";
    if (!toolName) return;
    const argStr =
      typeof args === "string"
        ? args
        : args == null
          ? "{}"
          : (() => {
              try {
                return JSON.stringify(args);
              } catch {
                return "{}";
              }
            })();
    const item: NormalizedToolCall = { call_id: callId, name: toolName, arguments: argStr };
    if (typeof thoughtSignature === "string" && thoughtSignature.trim()) {
      item.thought_signature = thoughtSignature.trim();
    }
    if (typeof thought === "string") {
      item.thought = thought;
    }
    out.push(item);
  };

  const msgObj = msg as Record<string, unknown>;
  const toolCalls = Array.isArray(msgObj.tool_calls) ? (msgObj.tool_calls as unknown[]) : [];
  for (const tc of toolCalls) {
    if (!tc || typeof tc !== "object") continue;
    const tcObj = tc as Record<string, unknown>;
    const id = typeof tcObj.id === "string" ? tcObj.id : "";
    const fn = tcObj.function && typeof tcObj.function === "object" ? (tcObj.function as Record<string, unknown>) : null;
    const name = fn && typeof fn.name === "string" ? fn.name : "";
    const args = fn ? fn.arguments : undefined;
    const thoughtSignature = tcObj.thought_signature ?? tcObj.thoughtSignature ?? fn?.thought_signature ?? fn?.thoughtSignature ?? "";
    const thought = tcObj.thought ?? tcObj.reasoning ?? fn?.thought ?? fn?.reasoning ?? "";
    pushToolCall(id, name, args, thoughtSignature, thought);
  }

  const fc = msgObj.function_call && typeof msgObj.function_call === "object" ? (msgObj.function_call as Record<string, unknown>) : null;
  if (fc) {
    const name = typeof fc.name === "string" ? fc.name : "";
    const args = "arguments" in fc ? fc.arguments : undefined;
    const thoughtSignature = fc.thought_signature ?? fc.thoughtSignature ?? "";
    const thought = fc.thought ?? fc.reasoning ?? "";
    pushToolCall(msgObj.tool_call_id || msgObj.id || "", name, args, thoughtSignature, thought);
  }

  return out;
}

export function appendInstructions(base: unknown, extra: unknown): string {
  const b = typeof base === "string" ? base.trim() : "";
  const e = typeof extra === "string" ? extra.trim() : "";
  if (!e) return b;
  if (!b) return e;
  if (b.includes(e)) return b;
  return `${b}\n\n${e}`;
}

export function sseHeaders(extraHeaders: Record<string, unknown> | undefined = undefined): Record<string, string> {
  const headers: Record<string, string> = {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
    "x-accel-buffering": "no",
    connection: "close",
  };
  if (extraHeaders && typeof extraHeaders === "object") {
    for (const [k, v] of Object.entries(extraHeaders)) {
      if (v == null) continue;
      headers[k] = String(v);
    }
  }
  return headers;
}

export function encodeSseData(dataStr: string): string {
  return `data: ${dataStr}\n\n`;
}

export async function sha256Hex(input: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(String(input ?? ""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export function getSessionKey(request: Request, reqJson: unknown, token: string): string {
  const headerKey = request.headers.get("x-session-id");
  if (typeof headerKey === "string" && headerKey.trim()) return headerKey.trim();
  const user = (reqJson && typeof reqJson === "object" ? (reqJson as Record<string, unknown>).user : undefined) as unknown;
  if (typeof user === "string" && user.trim()) return user.trim();

  // Best-effort: derive a stable per-conversation key when the client can't send `x-session-id`/`user`.
  // This prevents different chat threads from sharing the same cached `previous_response_id`.
  const messages = Array.isArray((reqJson && typeof reqJson === "object" ? (reqJson as Record<string, unknown>).messages : undefined))
    ? ((reqJson as Record<string, unknown>).messages as unknown[])
    : [];
  let firstUserText = "";
  for (const m of messages) {
    if (!m || typeof m !== "object") continue;
    const mObj = m as Record<string, unknown>;
    if (mObj.role !== "user") continue;
    const t = normalizeMessageContent(mObj.content);
    if (typeof t === "string" && t.trim()) {
      firstUserText = t.trim();
      break;
    }
  }
  if (firstUserText) {
    const model = typeof (reqJson && typeof reqJson === "object" ? (reqJson as Record<string, unknown>).model : undefined) === "string"
      ? String((reqJson as Record<string, unknown>).model).trim()
      : "";
    const fp = `${model}\n${firstUserText}`.slice(0, 512);
    return token ? `${token}|${fp}` : fp;
  }

  return token || "";
}
