import type { Env } from "./common";
import {
  bearerToken,
  getWorkerAuthKeys,
  isDebugEnabled,
  jsonError,
  jsonResponse,
  logDebug,
  normalizeAuthValue,
  previewString,
} from "./common";
import { claudeMessagesRequestToOpenaiChat, handleClaudeCountTokens, openaiChatResponseToClaudeMessage, openaiStreamToClaudeMessagesSse } from "./claude_api";
import { getProviderApiKey, parseGatewayConfig } from "./config";
import { dispatchOpenAIChatToProvider } from "./dispatch";
import { resolveModel } from "./model_resolver";
import { geminiModelsList, openaiModelsList } from "./models_list";
import { geminiRequestToOpenAIChat, openAIChatResponseToGemini } from "./protocols/gemini";
import { openAIChatResponseToResponses, responsesRequestToOpenAIChat } from "./protocols/responses";
import { openAIChatSseToGeminiSse, openAIChatSseToResponsesSse } from "./protocols/stream";

function getCorsHeaders(request: Request): Record<string, string> {
  const origin = request.headers.get("origin") || "";
  const allowOrigin = origin && typeof origin === "string" ? origin : "*";
  return {
    "access-control-allow-origin": allowOrigin,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "authorization,content-type,x-api-key,x-goog-api-key,anthropic-version,anthropic-beta",
    "access-control-max-age": "86400",
    vary: "Origin",
  };
}

function withCors(resp: Response, corsHeaders: Record<string, string>): Response {
  const headers = new Headers(resp.headers);
  for (const [k, v] of Object.entries(corsHeaders || {})) {
    if (v == null) continue;
    if (!headers.has(k)) headers.set(k, v);
  }
  return new Response(resp.body, { status: resp.status || 200, headers });
}

async function readJsonBody(request: Request): Promise<{ ok: true; value: any } | { ok: false; value: null }> {
  try {
    return { ok: true, value: await request.json() };
  } catch {
    return { ok: false, value: null };
  }
}

function joinUrls(urls: unknown): string {
  const list = Array.isArray(urls) ? urls.map((u) => String(u ?? "").trim()).filter(Boolean) : [];
  return list.join(",");
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    let reqId = "";
    try {
      reqId = crypto.randomUUID();
    } catch {
      reqId = `req_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
    }

    const debug = isDebugEnabled(env);
    const corsHeaders = getCorsHeaders(request);

    try {
      const url = new URL(request.url);
      const path = url.pathname.replace(/\/+$/, "") || "/";
      const startedAt = Date.now();

      // Allow browser preflight without auth (the preflight will not include Authorization).
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: corsHeaders });
      }

      if (debug) {
        const authHeader = request.headers.get("authorization") || "";
        const hasBearer = typeof authHeader === "string" && authHeader.toLowerCase().includes("bearer ");
        const xApiKey = request.headers.get("x-api-key") || "";
        logDebug(debug, reqId, "inbound request", {
          method: request.method,
          host: url.host,
          path,
          search: url.search || "",
          userAgent: request.headers.get("user-agent") || "",
          cfRay: request.headers.get("cf-ray") || "",
          hasAuthorization: Boolean(authHeader),
          authScheme: hasBearer ? "bearer" : authHeader ? "custom" : "",
          authorizationLen: typeof authHeader === "string" ? authHeader.length : 0,
          hasXApiKey: Boolean(xApiKey),
          xApiKeyLen: typeof xApiKey === "string" ? xApiKey.length : 0,
          contentType: request.headers.get("content-type") || "",
          contentLength: request.headers.get("content-length") || "",
        });
      }

      const workerAuthKeys = getWorkerAuthKeys(env);
      if (!workerAuthKeys.length) {
        return jsonResponse(
          500,
          jsonError("Server misconfigured: missing WORKER_AUTH_KEY/WORKER_AUTH_KEYS", "server_error"),
          corsHeaders,
        );
      }

      const authHeader = request.headers.get("authorization");
      let token = bearerToken(authHeader);
      if (!token && typeof authHeader === "string") {
        const maybe = authHeader.trim();
        if (maybe && !maybe.includes(" ")) token = maybe;
      }
      if (!token) token = request.headers.get("x-api-key");
      if (!token && path.startsWith("/gemini/")) {
        // Some Gemini clients use Google-style API key placement.
        if (!token) token = request.headers.get("x-goog-api-key");
        if (!token) token = url.searchParams.get("key");
      }
      token = normalizeAuthValue(token);

      if (!token) {
        return jsonResponse(401, jsonError("Missing API key", "unauthorized"), { ...corsHeaders, "www-authenticate": "Bearer" });
      }
      if (!workerAuthKeys.includes(token)) {
        return jsonResponse(401, jsonError("Unauthorized", "unauthorized"), { ...corsHeaders, "www-authenticate": "Bearer" });
      }

      if (debug) {
        logDebug(debug, reqId, "auth ok", { tokenLen: token.length, authKeyCount: workerAuthKeys.length });
      }

      const gatewayCfg = parseGatewayConfig(env);
      if (!gatewayCfg.ok || !gatewayCfg.config) {
        return jsonResponse(500, jsonError(gatewayCfg.error || "Server misconfigured: missing ANY_API_CONFIG", "server_error"), corsHeaders);
      }

      if (request.method === "GET" && (path === "/health" || path === "/v1/health")) {
        return jsonResponse(200, { ok: true, time: Math.floor(Date.now() / 1000) }, corsHeaders);
      }

      // Models list
      if (
        request.method === "GET" &&
        (path === "/v1/models" || path === "/models" || path === "/openai/v1/models" || path === "/claude/v1/models")
      ) {
        return jsonResponse(200, openaiModelsList(gatewayCfg.config), corsHeaders);
      }
      if (request.method === "GET" && path === "/gemini/v1beta/models") {
        return jsonResponse(200, geminiModelsList(gatewayCfg.config), corsHeaders);
      }

      // OpenAI Chat Completions
      if (request.method === "POST" && (path === "/v1/chat/completions" || path === "/chat/completions")) {
        const parsed = await readJsonBody(request);
        if (!parsed.ok || !parsed.value || typeof parsed.value !== "object") {
          return jsonResponse(400, jsonError("Invalid JSON body"), corsHeaders);
        }
        const reqJson = parsed.value as Record<string, any>;

        const resolved = resolveModel(gatewayCfg.config, reqJson.model);
        if (!resolved.ok) return jsonResponse(resolved.status, resolved.error, corsHeaders);

        reqJson.model = resolved.model.upstreamModel;
        reqJson.__client_token = token;
        const stream = Boolean(reqJson.stream);

        const resp = await dispatchOpenAIChatToProvider({
          request,
          env,
          config: gatewayCfg.config,
          provider: resolved.provider,
          model: resolved.model,
          reqJson,
          stream,
          debug,
          reqId,
          path,
          startedAt,
        });
        return withCors(resp, corsHeaders);
      }

      // Claude Messages
      if (request.method === "POST" && path === "/claude/v1/messages") {
        const parsed = await readJsonBody(request);
        if (!parsed.ok || !parsed.value || typeof parsed.value !== "object") {
          return jsonResponse(400, jsonError("Invalid JSON body"), corsHeaders);
        }
        const claudeReq = parsed.value as Record<string, any>;

        const resolved = resolveModel(gatewayCfg.config, claudeReq.model);
        if (!resolved.ok) return jsonResponse(resolved.status, resolved.error, corsHeaders);

        const converted = claudeMessagesRequestToOpenaiChat(claudeReq);
        if (!converted.ok) return jsonResponse(converted.status, converted.error, corsHeaders);

        const openaiReq = converted.req as Record<string, any>;
        openaiReq.model = resolved.model.upstreamModel;
        openaiReq.__client_token = token;
        const stream = Boolean(openaiReq.stream);

        const openaiResp = await dispatchOpenAIChatToProvider({
          request,
          env,
          config: gatewayCfg.config,
          provider: resolved.provider,
          model: resolved.model,
          reqJson: openaiReq,
          stream,
          debug,
          reqId,
          path,
          startedAt,
        });

        if (stream) {
          const transformed = await openaiStreamToClaudeMessagesSse(openaiResp, { reqModel: resolved.model.upstreamModel, debug, reqId });
          if (!transformed.ok) return jsonResponse(transformed.status, transformed.error, corsHeaders);
          return withCors(transformed.resp, corsHeaders);
        }

        const openaiJson = await openaiResp.json().catch(() => null);
        if (!openaiJson || typeof openaiJson !== "object") return withCors(openaiResp, corsHeaders);
        const claudeOut = openaiChatResponseToClaudeMessage(openaiJson);
        return jsonResponse(200, claudeOut, corsHeaders);
      }

      if (request.method === "POST" && path === "/claude/v1/messages/count_tokens") {
        const parsed = await readJsonBody(request);
        if (!parsed.ok || !parsed.value || typeof parsed.value !== "object") {
          return jsonResponse(400, jsonError("Invalid JSON body"), corsHeaders);
        }
        const reqJson = parsed.value as Record<string, any>;
        const resolved = resolveModel(gatewayCfg.config, reqJson.model);
        if (!resolved.ok) return jsonResponse(resolved.status, resolved.error, corsHeaders);
        reqJson.model = resolved.model.upstreamModel;

        const apiKey = getProviderApiKey(env, resolved.provider);
        const providerType = typeof resolved.provider.type === "string" ? resolved.provider.type.trim() : "";
        const messagesPath =
          resolved.provider.endpoints && typeof (resolved.provider.endpoints as any).messagesPath === "string"
            ? String((resolved.provider.endpoints as any).messagesPath).trim()
            : "";

        const env2: Env =
          providerType === "claude" && apiKey
            ? {
                ...env,
                CLAUDE_BASE_URL: joinUrls(resolved.provider.baseURLs),
                CLAUDE_API_KEY: apiKey,
                ...(messagesPath ? { CLAUDE_MESSAGES_PATH: messagesPath } : null),
              }
            : env;

        const resp = await handleClaudeCountTokens({ request, env: env2, reqJson, debug, reqId });
        return withCors(resp, corsHeaders);
      }

      // OpenAI Responses
      if (request.method === "POST" && path === "/openai/v1/responses") {
        const parsed = await readJsonBody(request);
        if (!parsed.ok || !parsed.value || typeof parsed.value !== "object") {
          return jsonResponse(400, jsonError("Invalid JSON body"), corsHeaders);
        }
        const respReq = parsed.value as Record<string, any>;
        const resolved = resolveModel(gatewayCfg.config, respReq.model);
        if (!resolved.ok) return jsonResponse(resolved.status, resolved.error, corsHeaders);

        const openaiReq = responsesRequestToOpenAIChat(respReq) as Record<string, any>;
        openaiReq.model = resolved.model.upstreamModel;
        openaiReq.__client_token = token;
        const stream = Boolean(respReq.stream);
        openaiReq.stream = stream;

        const openaiResp = await dispatchOpenAIChatToProvider({
          request,
          env,
          config: gatewayCfg.config,
          provider: resolved.provider,
          model: resolved.model,
          reqJson: openaiReq,
          stream,
          debug,
          reqId,
          path,
          startedAt,
        });

        const modelId = typeof respReq.model === "string" ? respReq.model : "";

        if (stream) {
          const body = openAIChatSseToResponsesSse(openaiResp, modelId);
          return withCors(new Response(body, { status: 200, headers: { "content-type": "text/event-stream; charset=utf-8" } }), corsHeaders);
        }

        const openaiJson = await openaiResp.json().catch(() => null);
        if (!openaiJson || typeof openaiJson !== "object") return withCors(openaiResp, corsHeaders);
        return jsonResponse(200, openAIChatResponseToResponses(openaiJson, modelId), corsHeaders);
      }

      // Gemini
      if (request.method === "POST" && path.startsWith("/gemini/v1beta/models/")) {
        const m = path.match(/^\/gemini\/v1beta\/models\/([^/]+):(generateContent|streamGenerateContent)$/);
        if (!m) return jsonResponse(404, jsonError("Not found", "not_found"), corsHeaders);

        const modelId = decodeURIComponent(m[1] || "");
        const methodName = m[2] || "generateContent";
        const stream = methodName === "streamGenerateContent";

        const resolved = resolveModel(gatewayCfg.config, modelId);
        if (!resolved.ok) return jsonResponse(resolved.status, resolved.error, corsHeaders);

        const parsed = await readJsonBody(request);
        if (!parsed.ok || !parsed.value || typeof parsed.value !== "object") {
          return jsonResponse(400, jsonError("Invalid JSON body"), corsHeaders);
        }

        const openaiReq = geminiRequestToOpenAIChat(parsed.value) as Record<string, any>;
        openaiReq.model = resolved.model.upstreamModel;
        openaiReq.stream = stream;
        openaiReq.__client_token = token;

        const openaiResp = await dispatchOpenAIChatToProvider({
          request,
          env,
          config: gatewayCfg.config,
          provider: resolved.provider,
          model: resolved.model,
          reqJson: openaiReq,
          stream,
          debug,
          reqId,
          path,
          startedAt,
        });

        if (stream) {
          const body = openAIChatSseToGeminiSse(openaiResp);
          return withCors(new Response(body, { status: 200, headers: { "content-type": "text/event-stream; charset=utf-8" } }), corsHeaders);
        }

        const openaiJson = await openaiResp.json().catch(() => null);
        if (!openaiJson || typeof openaiJson !== "object") return withCors(openaiResp, corsHeaders);
        return jsonResponse(200, openAIChatResponseToGemini(openaiJson), corsHeaders);
      }

      if (request.method !== "POST") {
        return jsonResponse(404, jsonError("Not found", "not_found"), corsHeaders);
      }
      return jsonResponse(404, jsonError("Not found", "not_found"), corsHeaders);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err ?? "unknown error");
      logDebug(debug, reqId, "unhandled exception", { error: message });
      return jsonResponse(500, jsonError("Internal server error", "server_error"), corsHeaders);
    }
  },
};

