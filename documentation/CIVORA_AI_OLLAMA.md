# CIVORA AI - Ollama Runtime

## Runtime flow

Frontend
-> `POST /api/ai/chat`
-> CIVORA backend
-> CIVORA agent service
-> Ollama local API
-> Qwen3-8B
-> CIVORA tools
-> Database-backed result
-> Final answer

## Environment

```env
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=qwen3:8b
OLLAMA_TIMEOUT_MS=45000
```

## Notes

- The backend keeps authorization authoritative.
- The model never talks to the database directly.
- Existing sales and forecast tools are preserved.
- The AI health endpoint is `GET /api/ai/health`.
