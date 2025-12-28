import { jsonError, jsonResponse, normalizeAuthValue } from "./common";
import type { Env, GatewayConfig, ModelConfig, ProviderConfig } from "./config";
import { getProviderApiKey } from "./config";
import { handleClaudeChatCompletions } from "./providers/claude";
import { handleGeminiChatCompletions } from "./providers/gemini";
import { handleOpenAIChatCompletionsUpstream, handleOpenAIRequest } from "./providers/openai";

function joinUrls(urls: unknown): string {
  const list = Array.isArray(urls) ? urls.map((u) => String(u ?? "").trim()).filter(Boolean) : [];
  return list.join(",");
}

function envWithOverrides(env: Env, overrides: Record<string, string> | null): Env {
  const base = env && typeof env === "object" ? env : {};
  return { ...base, ...(overrides && typeof overrides === "object" ? overrides : {}) };
}

function truthy(v: unknown): boolean {
  return v === true || v === 1 || (typeof v === "string" && ["1", "true", "yes", "on"].includes(v.trim().toLowerCase()));
}

function pickNumber(...values: unknown[]): number | null {
  for (const v of values) {
    if (v == null) continue;
    const n = typeof v === "number" ? v : Number(String(v));
    if (Number.isFinite(n) && n >= 0) return Math.floor(n);
  }
  return null;
}

export async function dispatchOpenAIChatToProvider({
  request,
  env,
  config,
  provider,
  model,
  reqJson,
  stream,
  debug,
  reqId,
  path,
  startedAt,
}: {
  request: Request;
  env: Env;
  config: GatewayConfig;
  provider: ProviderConfig;
  model: ModelConfig;
  reqJson: Record<string, unknown>;
  stream: boolean;
  debug: boolean;
  reqId: string;
  path?: string;
  startedAt?: number;
}): Promise<Response> {
  const apiKey = getProviderApiKey(env, provider);
  if (!apiKey) return jsonResponse(500, jsonError(`Server misconfigured: missing upstream API key for provider ${provider.id}`, "server_error"));

  const providerType = typeof provider.type === "string" ? provider.type.trim() : "";

  if (providerType === "openai-responses") {
    const quirks = provider.quirks || {};
    const providerOpts = provider?.options || {};
    const modelOpts = model?.options || {};
    const responsesPath =
      (provider.endpoints && typeof provider.endpoints.responsesPath === "string" && provider.endpoints.responsesPath.trim()) ||
      (provider.endpoints && typeof provider.endpoints.responses_path === "string" && provider.endpoints.responses_path.trim()) ||
      "";

    const upstreamUrls = joinUrls(provider.baseURLs);
    const noInstructionsUrls = truthy(quirks.noInstructions) ? upstreamUrls : "";
    const noPrevUrls = truthy(quirks.noPreviousResponseId) ? upstreamUrls : "";
    const reasoningEffort = typeof modelOpts.reasoningEffort === "string" ? modelOpts.reasoningEffort.trim() : "";
    const maxInstructionsChars = pickNumber(modelOpts.maxInstructionsChars, providerOpts.maxInstructionsChars);

    const env2 = envWithOverrides(env, {
      OPENAI_BASE_URL: upstreamUrls,
      OPENAI_API_KEY: apiKey,
      ...(responsesPath ? { RESP_RESPONSES_PATH: responsesPath } : null),
      ...(noInstructionsUrls ? { RESP_NO_INSTRUCTIONS_URLS: noInstructionsUrls } : null),
      ...(noPrevUrls ? { RESP_NO_PREVIOUS_RESPONSE_ID_URLS: noPrevUrls } : null),
      ...(reasoningEffort ? { RESP_REASONING_EFFORT: reasoningEffort } : null),
      ...(maxInstructionsChars != null ? { RESP_MAX_INSTRUCTIONS_CHARS: String(maxInstructionsChars) } : null),
    });

    const token = normalizeAuthValue(reqJson?.__client_token || "");

    return await handleOpenAIRequest({
      request,
      env: env2,
      reqJson,
      model: model.upstreamModel,
      stream,
      token,
      debug,
      reqId,
      path: typeof path === "string" ? path : "",
      startedAt: typeof startedAt === "number" && Number.isFinite(startedAt) ? startedAt : Date.now(),
      isTextCompletions: false,
      extraSystemText: "",
    });
  }

  if (providerType === "openai-chat-completions") {
    const providerOpts = provider?.options || {};
    const modelOpts = model?.options || {};
    const chatCompletionsPath =
      (provider.endpoints && typeof (provider.endpoints as any).chatCompletionsPath === "string" && String((provider.endpoints as any).chatCompletionsPath).trim()) ||
      (provider.endpoints && typeof (provider.endpoints as any).chat_completions_path === "string" && String((provider.endpoints as any).chat_completions_path).trim()) ||
      "";
    const maxInstructionsChars = pickNumber(modelOpts.maxInstructionsChars, providerOpts.maxInstructionsChars);

    const env2 = envWithOverrides(env, {
      OPENAI_BASE_URL: joinUrls(provider.baseURLs),
      OPENAI_API_KEY: apiKey,
      ...(chatCompletionsPath ? { OPENAI_CHAT_COMPLETIONS_PATH: chatCompletionsPath } : null),
      ...(maxInstructionsChars != null ? { RESP_MAX_INSTRUCTIONS_CHARS: String(maxInstructionsChars) } : null),
    });

    const token = normalizeAuthValue(reqJson?.__client_token || "");

    return await handleOpenAIChatCompletionsUpstream({
      request,
      env: env2,
      reqJson,
      model: model.upstreamModel,
      stream,
      token,
      debug,
      reqId,
      path: typeof path === "string" ? path : "",
      startedAt: typeof startedAt === "number" && Number.isFinite(startedAt) ? startedAt : Date.now(),
      extraSystemText: "",
    });
  }

  if (providerType === "claude") {
    const providerOpts = provider?.options || {};
    const modelOpts = model?.options || {};
    const messagesPath = provider.endpoints && typeof provider.endpoints.messagesPath === "string" ? provider.endpoints.messagesPath.trim() : "";
    const claudeMaxTokens = pickNumber(modelOpts.maxTokens, modelOpts.maxOutputTokens, providerOpts.maxTokens, providerOpts.maxOutputTokens);
    const env2 = envWithOverrides(env, {
      CLAUDE_BASE_URL: joinUrls(provider.baseURLs),
      CLAUDE_API_KEY: apiKey,
      ...(messagesPath ? { CLAUDE_MESSAGES_PATH: messagesPath } : null),
      ...(claudeMaxTokens != null ? { CLAUDE_MAX_TOKENS: String(claudeMaxTokens) } : null),
    });

    return await handleClaudeChatCompletions({
      env: env2,
      reqJson,
      model: model.upstreamModel,
      stream,
      debug,
      reqId,
      extraSystemText: "",
    });
  }

  if (providerType === "gemini") {
    const env2 = envWithOverrides(env, {
      GEMINI_BASE_URL: joinUrls(provider.baseURLs),
      GEMINI_API_KEY: apiKey,
    });

    // token is only used for session key derivation in some paths; safe to pass empty.
    const token = normalizeAuthValue(reqJson?.__client_token || "");

    return await handleGeminiChatCompletions({
      request,
      env: env2,
      reqJson,
      model: model.upstreamModel,
      stream,
      token,
      debug,
      reqId,
      extraSystemText: "",
    });
  }

  return jsonResponse(500, jsonError(`Unsupported provider type: ${providerType}`, "server_error"));
}
