const express = require('express');
const { authenticateToken } = require('../middleware/auth');

const router = express.Router();
router.use(authenticateToken);

const IAM_TOKEN_URL = 'https://iam.cloud.ibm.com/identity/token';
const tokenCache = {
  accessToken: null,
  expiresAt: 0,
};

function normalizeMessages(messages) {
  if (!Array.isArray(messages)) return [];

  return messages
    .map((message) => ({
      role: message?.role === 'assistant' ? 'assistant' : 'user',
      content: String(message?.content || '').trim(),
    }))
    .filter((message) => message.content.length > 0)
    .slice(-20);
}

function getAgentId() {
  return process.env.CIVORA_AGENT_ID || process.env.WATSONX_ORCHESTRATE_AGENT_ID || process.env.SVORA_AGENT_ID;
}

function buildChatUrl() {
  const chatBase = String(process.env.WATSONX_ORCHESTRATE_CHAT_URL || '').trim();
  const agentId = getAgentId();

  if (!chatBase) {
    throw new Error('Missing required environment variable: WATSONX_ORCHESTRATE_CHAT_URL');
  }

  if (!agentId) {
    throw new Error('Missing required environment variable: CIVORA_AGENT_ID');
  }

  if (/\/api\/v1\/orchestrate\/[^/]+\/chat\/completions\/?$/.test(chatBase)) {
    return chatBase;
  }

  const base = chatBase.replace(/\/$/, '');
  return [
    `${base}/api/v1/orchestrate/${encodeURIComponent(agentId)}/chat/completions`,
    `${base}/v1/orchestrate/${encodeURIComponent(agentId)}/chat/completions`,
  ];
}

async function getIamAccessToken(apiKey) {
  const now = Date.now();
  if (tokenCache.accessToken && tokenCache.expiresAt > now + 30000) {
    return tokenCache.accessToken;
  }

  if (typeof fetch !== 'function') {
    throw new Error('Global fetch is not available in this runtime.');
  }

  const response = await fetch(IAM_TOKEN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      grant_type: 'urn:ibm:params:oauth:grant-type:apikey',
      apikey: apiKey,
    }).toString(),
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(
      `IBM IAM token request failed with status ${response.status}${errorText ? `: ${errorText.slice(0, 400)}` : ''}`
    );
  }

  const data = await response.json().catch(() => ({}));
  if (!data.access_token) {
    throw new Error('IBM IAM token response did not include an access token.');
  }

  const expiresInSeconds = Number(data.expires_in || 0);
  tokenCache.accessToken = String(data.access_token);
  tokenCache.expiresAt = now + Math.max(expiresInSeconds - 60, 60) * 1000;

  return tokenCache.accessToken;
}

async function callWatsonxOrchestrate(messages, context) {
  const apiKey = String(process.env.WATSONX_ORCHESTRATE_API_KEY || '').trim();
  if (!apiKey) {
    throw new Error('Missing required environment variable: WATSONX_ORCHESTRATE_API_KEY');
  }

  const chatUrls = buildChatUrl();
  const accessToken = await getIamAccessToken(apiKey);

  let lastError = null;
  let data = null;

  for (let index = 0; index < chatUrls.length; index += 1) {
    const chatUrl = chatUrls[index];
    const response = await fetch(chatUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({
        agent_id: getAgentId(),
        conversation_id: context?.conversationId || context?.sessionId,
        messages,
        context,
        stream: false,
      }),
    });

    if (response.ok) {
      data = await response.json().catch(() => ({}));
      break;
    }

    const errorText = await response.text().catch(() => '');
    lastError = new Error(
      `Watsonx Orchestrate request failed with status ${response.status}${
        errorText ? `: ${errorText.slice(0, 400)}` : ''
      }`
    );

    if (response.status !== 404 || index === chatUrls.length - 1) {
      throw lastError;
    }
  }

  const reply =
    data?.reply ||
    data?.message ||
    data?.answer ||
    data?.output ||
    data?.text ||
    data?.choices?.[0]?.message?.content ||
    data?.choices?.[0]?.delta?.content ||
    data?.choices?.[0]?.content ||
    data?.result?.reply ||
    data?.result?.message ||
    '';

  return {
    reply: String(reply || '').trim() || JSON.stringify(data),
    raw: data,
  };
}

router.post('/ai/chat', async (req, res) => {
  const messages = normalizeMessages(req.body?.messages);
  const context = req.body?.context && typeof req.body.context === 'object' ? req.body.context : {};

  if (messages.length === 0) {
    return res.status(400).json({ error: 'At least one chat message is required.' });
  }

  try {
    const orchestrateResult = await callWatsonxOrchestrate(messages, context);

    return res.json({
      reply: orchestrateResult.reply,
      source: 'watsonx-orchestrate',
      context_summary: {
        projectName: context.projectName || context.project_name || null,
        year: context.year || null,
      },
    });
  } catch (error) {
    return res.status(500).json({
      error: error.message || 'Unable to process the AI request.',
    });
  }
});

module.exports = router;
