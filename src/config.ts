import { jsonError, jsonResponse, normalizeAuthValue, normalizeBaseUrl } from "./common";
import { parseJsonc } from "./jsonc";

export type Env = Record<string, string | undefined>;

export interface ModelConfig {
  name: string;
  upstreamModel: string;
  options: Record<string, unknown>;
  quirks: Record<string, unknown>;
}

export interface ProviderConfig {
  id: string;
  type: string;
  baseURLs: string[];
  apiKeyEnv: string;
  apiKey: string;
  options: Record<string, unknown>;
  endpoints: Record<string, unknown>;
  quirks: Record<string, unknown>;
  models: Record<string, ModelConfig>;
}

export interface GatewayConfig {
  version: 1;
  providers: Record<string, ProviderConfig>;
}

function normalizeStringArrayOrString(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((v) => String(v ?? "").trim()).filter(Boolean);
  const s = typeof value === "string" ? value.trim() : value == null ? "" : String(value).trim();
  return s ? [s] : [];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function normalizeProviderConfig(id: string, raw: unknown): ProviderConfig {
  const obj = isPlainObject(raw) ? raw : {};
  const type = typeof obj.type === "string" ? obj.type.trim() : "";
  const baseURLs = normalizeStringArrayOrString(obj.baseURL ?? obj.baseUrl ?? obj.url);
  const apiKeyEnv = typeof obj.apiKeyEnv === "string" ? obj.apiKeyEnv.trim() : "";
  const apiKey = typeof obj.apiKey === "string" ? obj.apiKey.trim() : "";

  const options = isPlainObject(obj.options) ? obj.options : {};
  const endpoints = isPlainObject(obj.endpoints) ? obj.endpoints : {};
  const quirks = isPlainObject(obj.quirks) ? obj.quirks : {};
  const models = isPlainObject(obj.models) ? obj.models : {};

  return {
    id,
    type,
    baseURLs,
    apiKeyEnv,
    apiKey,
    options,
    endpoints,
    quirks,
    models: models as unknown as Record<string, ModelConfig>,
  };
}

function normalizeModelConfig(name: string, raw: unknown): ModelConfig {
  const obj = isPlainObject(raw) ? raw : {};
  const upstreamModel = typeof obj.upstreamModel === "string" ? obj.upstreamModel.trim() : "";
  const options = isPlainObject(obj.options) ? obj.options : {};
  const quirks = isPlainObject(obj.quirks) ? obj.quirks : {};
  return { name, upstreamModel: upstreamModel || name, options, quirks };
}

export function parseGatewayConfig(env: Env):
  | { ok: true; config: GatewayConfig; source: "env"; error: "" }
  | { ok: false; config: null; source: "none" | "env"; error: string } {
  const raw = typeof env?.ANY_API_CONFIG === "string" ? env.ANY_API_CONFIG : "";
  if (!raw.trim()) return { ok: false, config: null, source: "none", error: "Missing ANY_API_CONFIG" };

  const parsed = parseJsonc(raw);
  if (!parsed.ok) return { ok: false, config: null, source: "env", error: `Invalid ANY_API_CONFIG: ${parsed.error}` };

  const root = parsed.value;
  if (!isPlainObject(root)) return { ok: false, config: null, source: "env", error: "ANY_API_CONFIG must be a JSON object" };

  const version = Number(root.version ?? 1);
  if (!Number.isFinite(version) || version !== 1) return { ok: false, config: null, source: "env", error: "Unsupported config version" };

  const providersRaw = isPlainObject(root.providers) ? root.providers : null;
  if (!providersRaw) return { ok: false, config: null, source: "env", error: "Missing providers" };

  const providers: Record<string, ProviderConfig> = {};
  for (const [idRaw, pr] of Object.entries(providersRaw)) {
    const id = String(idRaw ?? "").trim();
    if (!id) continue;
    if (id.includes(".")) {
      return { ok: false, config: null, source: "env", error: `Provider id must not contain '.': ${id}` };
    }
    const p = normalizeProviderConfig(id, pr);
    if (!p.type) return { ok: false, config: null, source: "env", error: `Provider ${id}: missing type` };
    if (!p.baseURLs.length) return { ok: false, config: null, source: "env", error: `Provider ${id}: missing baseURL` };

    // Normalize base URLs early.
    p.baseURLs = p.baseURLs
      .map((u) => normalizeBaseUrl(u))
      .map((u) => u.trim())
      .filter(Boolean);
    if (!p.baseURLs.length) return { ok: false, config: null, source: "env", error: `Provider ${id}: invalid baseURL` };

    if (!p.apiKey && !p.apiKeyEnv) {
      return { ok: false, config: null, source: "env", error: `Provider ${id}: missing apiKey or apiKeyEnv` };
    }

    const modelMap: Record<string, ModelConfig> = {};
    for (const [mnRaw, mr] of Object.entries(isPlainObject(p.models) ? p.models : {})) {
      const mn = String(mnRaw ?? "").trim();
      if (!mn) continue;
      modelMap[mn] = normalizeModelConfig(mn, mr);
    }
    p.models = modelMap;

    providers[id] = p;
  }

  if (!Object.keys(providers).length) return { ok: false, config: null, source: "env", error: "No providers configured" };

  return { ok: true, config: { version: 1, providers }, source: "env", error: "" };
}

export function getProviderApiKey(env: Env, provider: ProviderConfig): string {
  const inline = typeof provider?.apiKey === "string" ? provider.apiKey.trim() : "";
  if (inline) return normalizeAuthValue(inline);
  const keyName = typeof provider?.apiKeyEnv === "string" ? provider.apiKeyEnv.trim() : "";
  if (!keyName) return "";
  return normalizeAuthValue(env?.[keyName]);
}

export function configErrorResponse(status: number, message: string): Response {
  return jsonResponse(status, jsonError(message, status === 401 ? "unauthorized" : "server_error"));
}
