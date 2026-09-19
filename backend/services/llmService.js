const DEFAULT_BASE_URL = 'http://localhost:11434';
const DEFAULT_MODEL = 'qwen3:8b';
const DEFAULT_TIMEOUT_MS = 45000;
const DEFAULT_CHAT_TIMEOUT_MS = 90000;
const DEFAULT_HEALTH_TIMEOUT_MS = 15000;

function normalizeBaseUrl(value) {
  const text = String(value || '').trim();
  if (!text) return DEFAULT_BASE_URL;
  return text.replace(/\/$/, '');
}

function getLlmConfig() {
  const provider = String(process.env.LLM_PROVIDER || 'ollama').trim().toLowerCase();
  if (provider !== 'ollama') {
    const error = new Error(`Unsupported LLM provider: ${provider || 'unknown'}`);
    error.code = 'LLM_PROVIDER_UNSUPPORTED';
    throw error;
  }

  const baseUrl = normalizeBaseUrl(process.env.OLLAMA_BASE_URL || DEFAULT_BASE_URL);
  const model = String(process.env.OLLAMA_MODEL || DEFAULT_MODEL).trim();
  const timeoutMs = Math.max(5000, Number(process.env.OLLAMA_TIMEOUT_MS || DEFAULT_TIMEOUT_MS));

  return {
    provider,
    baseUrl,
    model,
    timeoutMs,
    chatTimeoutMs: Math.max(5000, Number(process.env.OLLAMA_CHAT_TIMEOUT_MS || process.env.OLLAMA_TIMEOUT_MS || DEFAULT_CHAT_TIMEOUT_MS)),
    healthTimeoutMs: Math.max(5000, Number(process.env.OLLAMA_HEALTH_TIMEOUT_MS || process.env.OLLAMA_TIMEOUT_MS || DEFAULT_HEALTH_TIMEOUT_MS)),
  };
}

async function fetchJsonWithTimeout(url, options, timeoutMs, stage) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });

    const payload = await response.json().catch(() => ({}));
    return { response, payload };
  } catch (error) {
    if (error.name === 'AbortError') {
      const timeoutError = new Error('The local LLM request timed out.');
      timeoutError.code = 'OLLAMA_TIMEOUT';
      timeoutError.stage = stage || 'llm_request';
      throw timeoutError;
    }

    const requestError = new Error('The local LLM service is unavailable.');
    requestError.code = 'OLLAMA_UNAVAILABLE';
    requestError.stage = stage || 'llm_request';
    requestError.cause = error;
    throw requestError;
  } finally {
    clearTimeout(timeout);
  }
}

async function listOllamaModels({ timeoutMs, stage = 'model_list' } = {}) {
  const { baseUrl, timeoutMs: defaultTimeoutMs } = getLlmConfig();
  const requestTimeoutMs = Math.max(5000, Number(timeoutMs || defaultTimeoutMs));
  const { response, payload } = await fetchJsonWithTimeout(`${baseUrl}/api/tags`, { method: 'GET' }, requestTimeoutMs, stage);

  if (!response.ok) {
    const error = new Error('Unable to query local Ollama models.');
    error.code = 'OLLAMA_REQUEST_FAILED';
    error.status = response.status;
    error.providerMessage = payload?.error || null;
    error.stage = stage;
    throw error;
  }

  return Array.isArray(payload?.models) ? payload.models : [];
}

async function ensureModelAvailable(modelName, { timeoutMs, stage = 'model_check' } = {}) {
  const model = String(modelName || getLlmConfig().model).trim();
  const models = await listOllamaModels({ timeoutMs, stage });
  const available = models.some((entry) => {
    const entryName = String(entry?.name || '').trim();
    return entryName === model || entryName.startsWith(`${model}:`);
  });

  if (!available) {
    const error = new Error(`The configured model ${model} is not installed in Ollama.`);
    error.code = 'OLLAMA_MODEL_NOT_FOUND';
    error.model = model;
    throw error;
  }

  return model;
}

function normalizeMessage(message) {
  if (!message || typeof message !== 'object') return null;

  const normalized = {
    role: message.role,
    content: String(message.content || ''),
  };

  if (Array.isArray(message.tool_calls) && message.tool_calls.length > 0) {
    normalized.tool_calls = message.tool_calls;
  }

  if (message.tool_name) {
    normalized.tool_name = message.tool_name;
  }

  return normalized;
}

async function createChatCompletion({ messages, tools = [], stream = false, timeoutMs: timeoutOverride } = {}) {
  const { baseUrl, model, timeoutMs, chatTimeoutMs } = getLlmConfig();
  const requestTimeoutMs = Math.max(5000, Number(timeoutOverride || chatTimeoutMs || timeoutMs));
  await ensureModelAvailable(model, { timeoutMs: requestTimeoutMs, stage: 'model_check' });

  const payload = {
    model,
    messages: Array.isArray(messages) ? messages.map(normalizeMessage).filter(Boolean) : [],
    tools: tools.length > 0 ? tools : undefined,
    stream: Boolean(stream),
    options: {
      temperature: 0.2,
    },
  };

  const { response, payload: responsePayload } = await fetchJsonWithTimeout(
    `${baseUrl}/api/chat`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    },
    requestTimeoutMs,
    'chat_request'
  );

  if (!response.ok) {
    const error = new Error('The local LLM service returned an error.');
    error.code = response.status === 404 ? 'OLLAMA_MODEL_NOT_FOUND' : 'OLLAMA_REQUEST_FAILED';
    error.status = response.status;
    error.providerMessage = responsePayload?.error || responsePayload?.message || null;
    error.stage = 'chat_request';
    throw error;
  }

  const message = responsePayload?.message;
  if (!message || typeof message !== 'object') {
    const error = new Error('The local LLM returned a malformed response.');
    error.code = 'OLLAMA_MALFORMED_RESPONSE';
    throw error;
  }

  return {
    role: message.role || 'assistant',
    content: String(message.content || ''),
    tool_calls: Array.isArray(message.tool_calls) ? message.tool_calls : undefined,
  };
}

async function probeLlmHealth() {
  const { model, healthTimeoutMs } = getLlmConfig();
  const configuredModel = await ensureModelAvailable(model, { timeoutMs: healthTimeoutMs, stage: 'health_model_check' });
  const message = await createChatCompletion({
    messages: [
      {
        role: 'user',
        content: 'Reply with a single word: ok.',
      },
    ],
    timeoutMs: healthTimeoutMs,
  });

  return {
    provider: 'ollama',
    model: configuredModel,
    model_available: true,
    can_generate: Boolean(String(message.content || '').trim()),
  };
}

module.exports = {
  createChatCompletion,
  ensureModelAvailable,
  getLlmConfig,
  listOllamaModels,
  probeLlmHealth,
};
