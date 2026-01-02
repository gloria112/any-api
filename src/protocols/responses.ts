import { normalizeMessageContent } from "../common";

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function partsToOpenAiContent(parts: unknown): Array<{ type: string; text?: string; image_url?: { url: string } }> | "" {
  const out = [];
  for (const p of Array.isArray(parts) ? parts : []) {
    if (!p || typeof p !== "object") continue;
    const t = (p as Record<string, unknown>).type;
    if (t === "input_text" && typeof p.text === "string") {
      out.push({ type: "text", text: p.text });
      continue;
    }
    if (t === "input_image") {
      const pObj = p as Record<string, unknown>;
      const imageUrl = pObj.image_url;
      const url =
        typeof imageUrl === "string"
          ? imageUrl
          : imageUrl && typeof imageUrl === "object" && typeof (imageUrl as Record<string, unknown>).url === "string"
            ? String((imageUrl as Record<string, unknown>).url)
            : "";
      if (typeof url === "string" && url) out.push({ type: "image_url", image_url: { url } });
      continue;
    }
  }
  if (!out.length) return "";
  // OpenAI Chat accepts either string or multimodal array; keep array.
  return out;
}

export function responsesRequestToOpenAIChat(reqJson: unknown): Record<string, unknown> {
  const body = isPlainObject(reqJson) ? reqJson : {};
  const instructions = typeof body.instructions === "string" ? body.instructions : "";
  const input = body.input;

  const messages: Array<{ role: string; content: unknown }> = [];
  // Normalize Responses `instructions` into Chat "developer" (newer convention).
  // Historically this was "system"; accept old clients elsewhere by mapping system->developer.
  if (instructions && instructions.trim()) messages.push({ role: "developer", content: instructions.trim() });

  if (typeof input === "string") {
    if (input.trim()) messages.push({ role: "user", content: input });
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (!item || typeof item !== "object") continue;
      const itemObj = item as Record<string, unknown>;
      const role =
        itemObj.role === "assistant" ? "assistant" : itemObj.role === "developer" || itemObj.role === "system" ? "developer" : "user";
      const content = Array.isArray(itemObj.content) ? partsToOpenAiContent(itemObj.content) : normalizeMessageContent(itemObj.content);
      messages.push({ role, content });
    }
  }

  const tools = Array.isArray(body.tools) ? body.tools : undefined;
  const toolChoice = body.tool_choice ?? undefined;

  return {
    model: typeof body.model === "string" ? body.model : "",
    messages,
    tools,
    tool_choice: toolChoice,
    stream: Boolean(body.stream),
    temperature: body.temperature,
    top_p: body.top_p,
    max_tokens: body.max_output_tokens ?? body.max_tokens,
  };
}

export function openAIChatResponseToResponses(chatJson: unknown, model: string): Record<string, unknown> {
  const chatObj = chatJson && typeof chatJson === "object" ? (chatJson as Record<string, unknown>) : {};
  const choices = Array.isArray(chatObj.choices) ? (chatObj.choices as unknown[]) : [];
  const choice0 = choices.length ? (choices[0] as Record<string, unknown>) : null;
  const msg = choice0 && typeof choice0.message === "object" ? (choice0.message as Record<string, unknown>) : null;
  const text = msg ? normalizeMessageContent(msg.content) : "";

  return {
    id: (typeof chatObj.id === "string" && chatObj.id.trim() ? chatObj.id : `resp_${Date.now().toString(36)}`),
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    model: model || "",
    output_text: text || "",
    output: [
      {
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: text || "" }],
      },
    ],
    usage: chatObj.usage ?? undefined,
  };
}
