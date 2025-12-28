import { normalizeMessageContent } from "../common";

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function geminiPartsToOpenAiContent(parts: unknown): Array<{ type: string; text?: string; image_url?: { url: string } }> | "" {
  const out = [];
  for (const p of Array.isArray(parts) ? parts : []) {
    if (!p || typeof p !== "object") continue;
    const pObj = p as Record<string, unknown>;
    if (typeof pObj.text === "string") {
      out.push({ type: "text", text: pObj.text });
      continue;
    }
    const inlineData = isPlainObject(pObj.inlineData) ? (pObj.inlineData as Record<string, unknown>) : null;
    if (inlineData && typeof inlineData.data === "string" && inlineData.data.trim()) {
      const mt =
        typeof inlineData.mimeType === "string" && inlineData.mimeType.trim() ? inlineData.mimeType.trim() : "application/octet-stream";
      const url = `data:${mt};base64,${inlineData.data.trim()}`;
      out.push({ type: "image_url", image_url: { url } });
      continue;
    }
    const fileData = isPlainObject(pObj.fileData) ? (pObj.fileData as Record<string, unknown>) : null;
    if (fileData && typeof fileData.fileUri === "string" && fileData.fileUri.trim()) {
      out.push({ type: "image_url", image_url: { url: fileData.fileUri.trim() } });
      continue;
    }
  }
  if (!out.length) return "";
  return out;
}

function geminiToolsToOpenAiTools(tools: unknown): Array<{ type: "function"; function: { name: string; description: string; parameters: unknown } }> {
  const list = Array.isArray(tools) ? tools : [];
  const out: Array<{ type: "function"; function: { name: string; description: string; parameters: unknown } }> = [];
  for (const t of list) {
    const decls = isPlainObject(t) && Array.isArray((t as Record<string, unknown>).functionDeclarations) ? ((t as Record<string, unknown>).functionDeclarations as unknown[]) : null;
    if (!decls) continue;
    for (const fd of decls) {
      if (!fd || typeof fd !== "object") continue;
      const fdObj = fd as Record<string, unknown>;
      const name = typeof fdObj.name === "string" ? fdObj.name.trim() : "";
      if (!name) continue;
      const description = typeof fdObj.description === "string" ? fdObj.description : "";
      const parameters = isPlainObject(fdObj.parameters) ? fdObj.parameters : { type: "object", properties: {} };
      out.push({ type: "function", function: { name, description, parameters } });
    }
  }
  return out;
}

export function geminiRequestToOpenAIChat(reqJson: unknown): Record<string, unknown> {
  const body = isPlainObject(reqJson) ? reqJson : {};
  const contents = Array.isArray(body.contents) ? (body.contents as unknown[]) : [];
  const systemInstruction = isPlainObject(body.systemInstruction) ? (body.systemInstruction as Record<string, unknown>) : null;

  const messages: Array<{ role: string; content: unknown }> = [];
  if (systemInstruction) {
    const parts = Array.isArray(systemInstruction.parts) ? (systemInstruction.parts as unknown[]) : [];
    const sysText = parts
      .map((p) => (p && typeof p === "object" && typeof (p as Record<string, unknown>).text === "string" ? String((p as Record<string, unknown>).text) : ""))
      .join("");
    if (sysText.trim()) messages.push({ role: "system", content: sysText.trim() });
  }

  for (const c of contents) {
    if (!c || typeof c !== "object") continue;
    const cObj = c as Record<string, unknown>;
    const role = cObj.role === "model" ? "assistant" : "user";
    const content = geminiPartsToOpenAiContent(cObj.parts);
    messages.push({ role, content });
  }

  const tools = geminiToolsToOpenAiTools(body.tools);

  const generationConfig = isPlainObject(body.generationConfig) ? body.generationConfig : {};
  const maxOutputTokens = (generationConfig as Record<string, unknown>).maxOutputTokens;
  const temperature = (generationConfig as Record<string, unknown>).temperature;
  const topP = (generationConfig as Record<string, unknown>).topP;

  return {
    model: "",
    messages,
    tools: tools.length ? tools : undefined,
    stream: false,
    max_tokens: maxOutputTokens,
    temperature,
    top_p: topP,
  };
}

export function openAIChatResponseToGemini(chatJson: unknown): Record<string, unknown> {
  const chatObj = chatJson && typeof chatJson === "object" ? (chatJson as Record<string, unknown>) : {};
  const choices = Array.isArray(chatObj.choices) ? (chatObj.choices as unknown[]) : [];
  const choice0 = choices.length ? (choices[0] as Record<string, unknown>) : null;
  const msg = choice0 && typeof choice0.message === "object" ? (choice0.message as Record<string, unknown>) : null;
  const text = msg ? normalizeMessageContent(msg.content) : "";
  const finishReason = choice0 && typeof choice0.finish_reason === "string" && choice0.finish_reason.trim() ? choice0.finish_reason : "STOP";

  return {
    candidates: [
      {
        content: {
          role: "model",
          parts: [{ text: text || "" }],
        },
        finishReason,
        index: 0,
      },
    ],
    usageMetadata:
      chatObj.usage && typeof chatObj.usage === "object"
        ? {
            promptTokenCount: Number((chatObj.usage as Record<string, unknown>).prompt_tokens ?? 0) || 0,
            candidatesTokenCount: Number((chatObj.usage as Record<string, unknown>).completion_tokens ?? 0) || 0,
            totalTokenCount: Number((chatObj.usage as Record<string, unknown>).total_tokens ?? 0) || 0,
          }
        : undefined,
  };
}
