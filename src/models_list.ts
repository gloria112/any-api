import type { GatewayConfig } from "./config";

export function listModelIds(config: GatewayConfig): string[] {
  const out: string[] = [];
  for (const [providerId, provider] of Object.entries(config.providers)) {
    for (const modelName of Object.keys(provider.models)) {
      const id = `${providerId}.${modelName}`;
      out.push(id);
    }
  }
  out.sort();
  return out;
}

export function openaiModelsList(config: GatewayConfig): { object: "list"; data: Array<{ id: string; object: "model"; created: number; owned_by: string }> } {
  const ids = listModelIds(config);
  return {
    object: "list",
    data: ids.map((id) => ({ id, object: "model", created: 0, owned_by: "any-api" })),
  };
}

export function geminiModelsList(config: GatewayConfig): {
  models: Array<{ name: string; displayName: string; supportedGenerationMethods: ["generateContent", "streamGenerateContent"] }>;
} {
  const ids = listModelIds(config);
  // Gemini style: { models: [{ name: "models/<id>", supportedGenerationMethods: [...] }], nextPageToken? }
  return {
    models: ids.map((id) => ({
      name: `models/${id}`,
      displayName: id,
      supportedGenerationMethods: ["generateContent", "streamGenerateContent"],
    })),
  };
}
