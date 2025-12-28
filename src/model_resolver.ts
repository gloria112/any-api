import { jsonError } from "./common";
import type { GatewayConfig, ModelConfig, ProviderConfig } from "./config";

export function splitProviderModel(
  modelId: unknown,
): { ok: true; providerId: string; modelName: string; error: "" } | { ok: false; providerId: ""; modelName: ""; error: string } {
  const raw = typeof modelId === "string" ? modelId.trim() : "";
  if (!raw) return { ok: false, providerId: "", modelName: "", error: "Missing model" };
  if (raw.includes(":")) return { ok: false, providerId: "", modelName: "", error: "Invalid model: use provider.modelName (':' is not allowed)" };
  const idx = raw.indexOf(".");
  if (idx <= 0 || idx === raw.length - 1) {
    return { ok: false, providerId: "", modelName: "", error: "Invalid model: must be provider.modelName" };
  }
  const providerId = raw.slice(0, idx).trim();
  const modelName = raw.slice(idx + 1).trim();
  if (!providerId || !modelName) return { ok: false, providerId: "", modelName: "", error: "Invalid model: must be provider.modelName" };
  return { ok: true, providerId, modelName, error: "" };
}

export function resolveModel(
  config: GatewayConfig,
  modelId: unknown,
):
  | {
      ok: true;
      providerId: string;
      modelName: string;
      provider: ProviderConfig;
      model: ModelConfig;
    }
  | { ok: false; status: number; error: { error: { message: string; type: string; code: string } } } {
  const split = splitProviderModel(modelId);
  if (!split.ok) {
    return { ok: false, status: 400, error: jsonError(split.error, "invalid_request_error") };
  }

  const provider = config.providers[split.providerId];
  if (!provider) {
    return { ok: false, status: 400, error: jsonError(`Unknown provider: ${split.providerId}`, "invalid_request_error") };
  }

  const model = provider.models[split.modelName];
  if (!model) {
    return { ok: false, status: 400, error: jsonError(`Unknown model for provider ${split.providerId}: ${split.modelName}`, "invalid_request_error") };
  }

  return { ok: true, providerId: split.providerId, modelName: split.modelName, provider, model };
}
