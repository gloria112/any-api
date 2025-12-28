# Any API Gateway (Cloudflare Worker)

在 Cloudflare Workers 上运行的通用 LLM API 互转/路由网关：同一套后端 provider 配置，同时暴露四种“前端协议”入口。

[推荐使用 Right.codes 中转服务](https://www.right.codes/register?aff=b9f13319)

**入口协议（四选一）**
- OpenAI Chat Completions：`POST /v1/chat/completions`
- OpenAI Responses：`POST /openai/v1/responses`
- Claude Messages：`POST /claude/v1/messages`、`POST /claude/v1/messages/count_tokens`
- Gemini：`POST /gemini/v1beta/models/{provider.modelName}:generateContent`、`POST /gemini/v1beta/models/{provider.modelName}:streamGenerateContent?alt=sse`

**模型列表**
- OpenAI 风格：`GET /v1/models`（同样支持 `/openai/v1/models`、`/claude/v1/models`）
- Gemini 风格：`GET /gemini/v1beta/models`

## 统一模型命名（必须）

所有入口协议里出现的模型名都必须是 `provider.modelName`，例如：
- `provider-id.model-name`

网关会用 `provider` 定位上游连接信息（URL / key / quirks / options），用 `modelName` 定位上游实际模型名（`upstreamModel`）。

## 入站鉴权（必须）

所有请求都需要携带你的 Worker 访问密钥（不是上游模型 key）：
- Worker 侧配置：`WORKER_AUTH_KEY` 或 `WORKER_AUTH_KEYS`（逗号分隔）
- 客户端传递方式：
  - `Authorization: Bearer <key>` 或 `Authorization: <key>`
  - `x-api-key: <key>`
  - Gemini 兼容：`x-goog-api-key: <key>` 或 `?key=<key>`（仅当路径以 `/gemini/` 开头）

## 配置（ANY_API_CONFIG）

唯一必需的网关配置是 `ANY_API_CONFIG`（JSON/JSONC 字符串），示例见：
- `.dev.vars.example`
- `wrangler.toml.example`

### ANY_API_CONFIG 示例

Right.codes（上游为 OpenAI Responses；部分能力不支持 `instructions` / `previous_response_id`）：
```jsonc
{
  "version": 1,
  "providers": {
    "rightcode": {
      "type": "openai-responses",
      "baseURL": "https://www.right.codes/codex",
      "apiKey": "REPLACE_ME",
      "quirks": {
        "noInstructions": true,
        "noPreviousResponseId": true
      },
      "models": {
        "main": {
          "upstreamModel": "gpt-5.2",
          "options": {
            "reasoningEffort": "high",
            "maxInstructionsChars": 12000
          }
        }
      }
    }
  }
}
```

同时配置 Right.codes + Google Gemini（两个上游 provider 并存）：
```jsonc
{
  "version": 1,
  "providers": {
    "rightcode": {
      "type": "openai-responses",
      "baseURL": "https://www.right.codes/codex",
      "apiKey": "REPLACE_ME",
      "quirks": { "noInstructions": true, "noPreviousResponseId": true },
      "models": { "main": { "upstreamModel": "gpt-5.2" } }
    },
    "google": {
      "type": "gemini",
      "baseURL": "https://generativelanguage.googleapis.com",
      "apiKey": "REPLACE_ME",
      "models": { "main": { "upstreamModel": "gemini-3-pro-preview" } }
    }
  }
}
```

常用 provider type：
- `openai-responses`：上游走 Responses API（支持 reasoning、tool calling、SSE 等）
- `openai-chat-completions`：上游走 Chat Completions API（`/v1/chat/completions`）
- `gemini`：上游走 Gemini `generateContent` / `streamGenerateContent`
- `claude`：上游走 Claude `/v1/messages`（当你把 OpenAI Chat 路由到 Claude provider 时使用）

## 本地运行

```bash
npm install
cp .dev.vars.example .dev.vars
npm run dev
```

## 快速 curl

OpenAI Chat:
```bash
curl -sS http://localhost:8787/v1/chat/completions \
  -H "Authorization: Bearer REPLACE_ME" \
  -H "Content-Type: application/json" \
  -d '{"model":"provider-id.model-name","messages":[{"role":"user","content":"hello"}]}'
```

OpenAI Responses:
```bash
curl -sS http://localhost:8787/openai/v1/responses \
  -H "Authorization: Bearer REPLACE_ME" \
  -H "Content-Type: application/json" \
  -d '{"model":"provider-id.model-name","input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}]}'
```

Claude Messages:
```bash
curl -sS http://localhost:8787/claude/v1/messages \
  -H "Authorization: Bearer REPLACE_ME" \
  -H "Content-Type: application/json" \
  -d '{"model":"provider-id.model-name","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}'
```

Gemini streaming:
```bash
curl -N http://localhost:8787/gemini/v1beta/models/provider-id.model-name:streamGenerateContent?alt=sse \
  -H "x-goog-api-key: REPLACE_ME" \
  -H "Content-Type: application/json" \
  -d '{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}'
```
