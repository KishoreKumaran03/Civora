const express = require('express');
const { authenticateToken } = require('../middleware/auth');
const { resolveCivoraAiContext } = require('../services/civoraAiContext');
const { runCivoraAgent } = require('../services/civoraAgentService');
const { probeLlmHealth } = require('../services/llmService');

const router = express.Router();

const OUTAGE_ERROR_CODES = new Set([
  'OLLAMA_TIMEOUT',
  'OLLAMA_UNAVAILABLE',
  'OLLAMA_REQUEST_FAILED',
  'OLLAMA_MALFORMED_RESPONSE',
  'OLLAMA_MODEL_NOT_FOUND',
]);

function getReadableStage(stage) {
  if (!stage) return 'the request';
  if (stage === 'model_check' || stage === 'health_model_check') return 'checking the model';
  if (stage === 'chat_request') return 'sending the prompt to Ollama';
  if (stage === 'final_synthesis') return 'generating the final answer';
  if (stage === 'tool_round_limit') return 'processing tool calls';
  if (String(stage).startsWith('tool_round_')) return 'processing tool calls';
  return stage.replace(/_/g, ' ');
}

function buildAiErrorPayload(error) {
  const stageText = getReadableStage(error.stage);

  if (error.code === 'OLLAMA_MODEL_NOT_FOUND') {
    return {
      statusCode: 503,
      body: {
        error: 'The local CIVORA AI model is not installed. Please make sure Ollama and Qwen3-8B are running.',
        code: error.code,
        stage: error.stage || null,
      },
    };
  }

  if (error.code === 'OLLAMA_TIMEOUT') {
    return {
      statusCode: 503,
      body: {
        error: `The local CIVORA AI request timed out while ${stageText}. Please try again.`,
        code: error.code,
        stage: error.stage || null,
      },
    };
  }

  if (error.code === 'OLLAMA_UNAVAILABLE' || error.code === 'OLLAMA_REQUEST_FAILED' || error.code === 'OLLAMA_MALFORMED_RESPONSE') {
    return {
      statusCode: 503,
      body: {
        error: `The local CIVORA AI service had trouble while ${stageText}. Please make sure Ollama and Qwen3-8B are running.`,
        code: error.code,
        stage: error.stage || null,
      },
    };
  }

  if (error.code === 'TOOL_ROUND_LIMIT') {
    return {
      statusCode: 503,
      body: {
        error: 'The assistant kept requesting tools and hit the tool-call limit. Please try rephrasing your question.',
        code: error.code,
        stage: error.stage || null,
      },
    };
  }

  return null;
}

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

router.post('/ai/chat', authenticateToken, async (req, res) => {
  const incomingMessages = Array.isArray(req.body?.messages)
    ? req.body.messages
    : (req.body?.message ? [{ role: 'user', content: req.body.message }] : []);
  const messages = normalizeMessages(incomingMessages);
  const requestContext = req.body?.context && typeof req.body.context === 'object' ? req.body.context : {};

  if (messages.length === 0) {
    return res.status(400).json({ error: 'At least one chat message is required.' });
  }

  try {
    const { aiContext, store, selectionRequired, availableStoreCount } = await resolveCivoraAiContext({
      userId: req.user.userId,
      requestContext,
    });

    if (!aiContext) {
      return res.status(404).json({
        error: 'The CIVORA user could not be found.',
        code: 'USER_NOT_FOUND',
      });
    }

    const agentResult = await runCivoraAgent({
      messages,
      aiContext,
      selectionRequired,
      availableStoreCount,
    });

    return res.json({
      reply: agentResult.reply,
      source: 'civora-yua',
      context_summary: {
        user_id: aiContext.user_id,
        store_id: aiContext.store_id,
        role: aiContext.role,
        currency: aiContext.currency,
        timezone: aiContext.timezone,
        store_name: store?.name || null,
        store_selection_required: selectionRequired,
        available_store_count: availableStoreCount,
        tools_used: agentResult.tools_used,
        tool_rounds: agentResult.tool_rounds,
        knowledge_used: Boolean(agentResult.knowledge_used),
        knowledge_sources: agentResult.knowledge_sources || [],
        intent_mode: agentResult.intent?.mode || null,
        intent_reason: agentResult.intent?.reason || null,
      },
    });
  } catch (error) {
    if (error.code === 'STORE_ACCESS_DENIED') {
      return res.status(403).json({
        error: 'You do not have access to that store.',
        code: error.code,
      });
    }

    if (error.code === 'STORE_NOT_FOUND') {
      return res.status(404).json({
        error: 'The selected store could not be found.',
        code: error.code,
      });
    }

    if (error.code === 'STORE_CONTEXT_REQUIRED' || error.code === 'INVALID_DATE_RANGE' || error.code === 'USER_CONTEXT_REQUIRED') {
      return res.status(400).json({
        error: error.message,
        code: error.code,
      });
    }

    if (error.code === 'LLM_PROVIDER_UNSUPPORTED') {
      return res.status(503).json({
        error: 'The CIVORA AI provider is not configured correctly.',
        code: error.code,
      });
    }

    const aiError = buildAiErrorPayload(error);
    if (aiError) {
      return res.status(aiError.statusCode).json(aiError.body);
    }

    return res.status(500).json({
      error: error.message || 'Unable to process the AI request.',
      code: error.code || null,
      stage: error.stage || null,
    });
  }
});

router.get('/ai/health', authenticateToken, async (_req, res) => {
  try {
    const health = await probeLlmHealth();
    return res.json({
      status: 'ok',
      source: 'civora-yua',
      ...health,
    });
  } catch (error) {
    if (error.code === 'LLM_PROVIDER_UNSUPPORTED') {
      return res.status(503).json({
        status: 'error',
        source: 'civora-yua',
        error: 'The CIVORA AI provider is not configured correctly.',
        code: error.code,
      });
    }

    if (error.code === 'OLLAMA_MODEL_NOT_FOUND') {
      return res.status(503).json({
        status: 'degraded',
        source: 'civora-yua',
        error: 'The local CIVORA AI model is not installed. Please make sure Ollama and Qwen3-8B are running.',
        code: error.code,
      });
    }

    if (error.code === 'OLLAMA_TIMEOUT' || error.code === 'OLLAMA_UNAVAILABLE' || error.code === 'OLLAMA_REQUEST_FAILED' || error.code === 'OLLAMA_MALFORMED_RESPONSE') {
      return res.status(503).json({
        status: 'degraded',
        source: 'civora-yua',
        error: `The local CIVORA AI service had trouble while ${getReadableStage(error.stage)}. Please make sure Ollama and Qwen3-8B are running.`,
        code: error.code,
        stage: error.stage || null,
      });
    }

    return res.status(500).json({
      status: 'error',
      source: 'civora-yua',
      error: 'Unable to check the local AI service.',
    });
  }
});

module.exports = router;
