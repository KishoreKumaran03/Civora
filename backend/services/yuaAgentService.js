const { createChatCompletion } = require('./llmService');
const { getSalesData } = require('./civoraAiService');
const {
  runAnalyticalReasoning,
  runExtremeReasoning,
  runStoreOverviewReasoning,
  shouldUseAnalyticalReasoning,
  shouldUseExtremeReasoning,
  shouldUseStoreOverviewReasoning,
  resolveExtremeRow,
  buildExtremeAnswerLines,
} = require('./yuaAnalyticsReasoningService');
const { runForecastReasoning, shouldUseForecastReasoning } = require('./yuaForecastReasoningService');
const { executeToolCall, toolDefinitions } = require('./toolRegistry');
const { formatKnowledgeContext, retrieveRelevantKnowledge } = require('./civoraKnowledgeService');
const { getYuaSystemPrompt } = require('./yuaPromptService');
const { formatYuaReply } = require('./yuaResponseFormatter');
const { sanitizeAssistantText } = require('./yuaValidation');

const MAX_TOOL_ROUNDS = 4;
const MAX_MESSAGES = 20;
const MAX_KNOWLEDGE_CHUNKS = 4;

const MONTH_NAME_ORDER = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

const MONTH_NAME_TO_INDEX = MONTH_NAME_ORDER.reduce((accumulator, monthName, index) => {
  accumulator[monthName] = index;
  return accumulator;
}, {});

const DOC_PHRASES = [
  'what is',
  'what does',
  'what are',
  'civora',
  'meaning of',
  'definition of',
  'how does civora',
  'how do i',
  'how to',
  'documentation',
  'feature',
  'features',
  'capability',
  'capabilities',
  'workflow',
  'api',
  'dashboard',
  'limitations',
  'metric',
  'metrics',
  'formula',
  'calculated',
  'calculate',
  'glossary',
  'help me understand',
  'explain civora',
  'purpose',
  'platform',
  'guide',
  'decision',
  'decisions',
  'functionality',
];

const LIVE_PHRASES = [
  'sales',
  'revenue',
  'profit',
  'cost',
  'forecast',
  'product',
  'products',
  'category',
  'categories',
  'store',
  'stores',
  'compare',
  'comparison',
  'trend',
  'trends',
  'month',
  'months',
  'last month',
  'this month',
  'top selling',
  'top product',
  'top products',
  'best performing',
  'performance',
  'units',
  'sell',
  'sold',
  'how much',
];

const BOTH_PHRASES = [
  'why did',
  'why is',
  'why are',
  'why was',
  'because',
  'cause',
  'caused',
  'explain why',
  'explain this',
  'analyze',
  'analysis',
  'compare',
];

function normalizeConversation(messages) {
  if (!Array.isArray(messages)) return [];

  return messages
    .filter((message) => message && (message.role === 'user' || message.role === 'assistant'))
    .map((message) => ({
      role: message.role,
      content: String(message.content || '').trim(),
    }))
    .filter((message) => message.content.length > 0)
    .slice(-MAX_MESSAGES);
}

function buildContextMessage(aiContext, selectionRequired, availableStoreCount) {
  const safeContext = {
    user_id: aiContext?.user_id || null,
    store_id: aiContext?.store_id || null,
    role: aiContext?.role || null,
    currency: aiContext?.currency || null,
    timezone: aiContext?.timezone || null,
    store_selection_required: Boolean(selectionRequired),
    available_store_count: Number(availableStoreCount || 0),
  };

  return {
    role: 'system',
    content: `Current CIVORA context (use only for deciding how to help; authorization is enforced by CIVORA): ${JSON.stringify(safeContext)}`,
  };
}

function normalizeText(value) {
  return String(value || '').toLowerCase();
}

function phraseScore(text, phrases) {
  return phrases.reduce((score, phrase) => score + (text.includes(phrase) ? 1 : 0), 0);
}

function getLatestUserMessage(conversation) {
  for (let index = conversation.length - 1; index >= 0; index -= 1) {
    if (conversation[index]?.role === 'user') {
      return conversation[index].content;
    }
  }
  return '';
}

function buildConversationTrace(conversation) {
  return conversation
    .slice(-6)
    .map((message) => `${message.role}: ${message.content}`)
    .join('\n');
}

function isAmbiguousPerformanceQuery(queryText, conversation) {
  const norm = normalizeText(queryText).trim().replace(/[?!.]+$/, '');
  const isVague = /^(how did (we|i|my store) perform( last month)?|how was (last month|our performance))$/i.test(norm);
  if (!isVague) return false;
  const trace = buildConversationTrace(conversation);
  const hasPriorMetric = /revenue|profit|cost|quantity|units|sales/i.test(trace);
  return !hasPriorMetric;
}

function extractMentionedMonths(text) {
  const normalized = normalizeText(text);
  return MONTH_NAME_ORDER.filter((monthName) => new RegExp(`\\b${monthName}\\b`, 'i').test(normalized));
}

function extractMentionedYear(text) {
  const match = String(text || '').match(/\b(20\d{2})\b/);
  return match ? Number(match[1]) : null;
}

function getSalesRowMonthIndex(row) {
  return MONTH_NAME_TO_INDEX[String(row?.month_name || '').toLowerCase()] ?? -1;
}

function findSalesRow(rows, monthName, year) {
  const targetMonthIndex = MONTH_NAME_TO_INDEX[String(monthName || '').toLowerCase()];
  if (targetMonthIndex == null || !Array.isArray(rows)) {
    return null;
  }

  const matchingRows = rows.filter((row) => getSalesRowMonthIndex(row) === targetMonthIndex);
  if (matchingRows.length === 0) {
    return null;
  }

  if (year != null) {
    const yearMatch = matchingRows.find((row) => Number(row?.year) === Number(year));
    if (yearMatch) {
      return yearMatch;
    }
  }

  return matchingRows.sort((left, right) => Number(right.year) - Number(left.year))[0] || null;
}

function capitalizeMonth(monthName) {
  const normalized = String(monthName || '').toLowerCase();
  return normalized ? `${normalized[0].toUpperCase()}${normalized.slice(1)}` : '';
}

function buildRevenueComparisonReply(rows, queryText) {
  const months = extractMentionedMonths(queryText);
  if (months.length < 2) {
    return null;
  }

  const year = extractMentionedYear(queryText);
  const firstMonth = months[0];
  const secondMonth = months[1];
  const firstRow = findSalesRow(rows, firstMonth, year);
  const secondRow = findSalesRow(rows, secondMonth, year);

  if (!firstRow || !secondRow) {
    return null;
  }

  const firstRevenue = Number(firstRow.total_revenue || 0);
  const secondRevenue = Number(secondRow.total_revenue || 0);
  const delta = secondRevenue - firstRevenue;
  const direction = delta >= 0 ? 'increased' : 'decreased';
  const base = Math.max(Math.abs(firstRevenue), 1);
  const percentChange = Math.abs((delta / base) * 100);
  const effectiveYear = year || Number(firstRow.year) || Number(secondRow.year) || null;
  const yearLabel = effectiveYear ? ` ${effectiveYear}` : '';
  const higherMonth = firstRevenue > secondRevenue ? firstMonth : secondRevenue > firstRevenue ? secondMonth : null;
  const higherLine = higherMonth ? `${capitalizeMonth(higherMonth)} was the higher month.` : 'Both months had identical revenue.';

  return [
    `${capitalizeMonth(firstMonth)}${yearLabel} revenue was ₹${firstRevenue.toLocaleString('en-IN')}.`,
    `${capitalizeMonth(secondMonth)}${yearLabel} revenue was ₹${secondRevenue.toLocaleString('en-IN')}.`,
    `Revenue ${direction} by ₹${Math.abs(delta).toLocaleString('en-IN')} (${percentChange.toFixed(1)}%).`,
    higherLine,
  ].join('\n');
}

function buildRevenueChangeExplanationReply(rows) {
  if (!Array.isArray(rows) || rows.length < 2) {
    return null;
  }

  const previousRow = rows[rows.length - 2];
  const currentRow = rows[rows.length - 1];
  const previousRevenue = Number(previousRow?.total_revenue || 0);
  const currentRevenue = Number(currentRow?.total_revenue || 0);
  const delta = currentRevenue - previousRevenue;
  const changePercent = Math.abs((delta / Math.max(Math.abs(previousRevenue), 1)) * 100);
  const direction = delta >= 0 ? 'increased' : 'decreased';

  const lines = [
    `From the available data, ${currentRow?.month_name || 'the latest month'} ${currentRow?.year || ''} revenue ${direction} by ₹${Math.abs(delta).toLocaleString('en-IN')} (${changePercent.toFixed(1)}%) compared with ${previousRow?.month_name || 'the previous month'} ${previousRow?.year || ''}.`.trim(),
  ];

  if (previousRow?.top_product || currentRow?.top_product) {
    lines.push(`Top product moved from ${previousRow.top_product || 'the previous month'} to ${currentRow.top_product || 'the current month'}.`);
  }

  if (previousRow?.top_region || currentRow?.top_region) {
    lines.push(`Top region moved from ${previousRow.top_region || 'the previous month'} to ${currentRow.top_region || 'the current month'}.`);
  }

  const previousNet = Number(previousRow?.net_revenue || 0);
  const currentNet = Number(currentRow?.net_revenue || 0);
  if (Number.isFinite(previousNet) && Number.isFinite(currentNet)) {
    const netDelta = currentNet - previousNet;
    lines.push(`Net revenue also ${netDelta >= 0 ? 'rose' : 'fell'} by ₹${Math.abs(netDelta).toLocaleString('en-IN')}.`);
  }

  lines.push('The main visible drivers are the month-over-month change in total revenue and the shift in supporting product or regional contribution.');
  return lines.join('\n');
}

function shouldHandleDirectRevenueAnalysis(intent, queryText) {
  const months = extractMentionedMonths(queryText);
  const query = normalizeText(queryText);
  const comparisonWords = query.includes('compare') || query.includes('comparison') || query.includes('vs') || query.includes('versus') || query.includes('difference');
  const causalWords = query.includes('why did') || query.includes('why is') || query.includes('why are') || query.includes('why was') || query.includes('why');
  const revenueWords = query.includes('revenue') || query.includes('sales') || query.includes('profit') || query.includes('decrease') || query.includes('decreased') || query.includes('drop') || query.includes('dropped') || query.includes('fall') || query.includes('fell') || query.includes('increase') || query.includes('increased');

  return Boolean(
    (months.length >= 2 && comparisonWords && (intent?.mode === 'both' || intent?.needsLiveData)) ||
    (causalWords && revenueWords && (intent?.mode === 'both' || intent?.needsLiveData))
  );
}

async function runDirectRevenueAnalysis({ queryText, aiContext }) {
  const salesData = await getSalesData({
    userContext: aiContext,
    storeId: aiContext?.store_id || aiContext?.storeId || null,
  });

  const months = extractMentionedMonths(queryText);
  const reply = months.length >= 2
    ? buildRevenueComparisonReply(salesData.monthly_rows, queryText)
    : buildRevenueChangeExplanationReply(salesData.monthly_rows);
  if (!reply) {
    const error = new Error('Unable to build a direct revenue analysis.');
    error.code = 'REVENUE_ANALYSIS_NOT_AVAILABLE';
    throw error;
  }

  return {
    reply: finalizeYuaReply(reply, { mode: 'live', needsLiveData: true }),
    tool_rounds: 0,
    tools_used: ['get_sales_data'],
  };
}

function classifyIntent(conversation) {
  const latest = normalizeText(getLatestUserMessage(conversation));
  const trace = normalizeText(buildConversationTrace(conversation));
  const contextText = `${latest} ${trace}`;
  const docScore = phraseScore(contextText, DOC_PHRASES);
  const liveScore = phraseScore(contextText, LIVE_PHRASES);
  const bothScore = phraseScore(contextText, BOTH_PHRASES);
  const shortFollowUp = latest.split(/\s+/).filter(Boolean).length <= 5;

  const docOnlySignals = docScore > 0 && liveScore === 0;
  const liveOnlySignals = liveScore > 0 && docScore === 0;
  const bothSignals = bothScore > 0 || (docScore > 0 && liveScore > 0);

  if (bothSignals) {
    return {
      mode: 'both',
      needsKnowledge: true,
      needsLiveData: true,
      docScore,
      liveScore,
      reason: 'question_needs_explanation_and_live_data',
    };
  }

  if (docOnlySignals) {
    return {
      mode: 'knowledge',
      needsKnowledge: true,
      needsLiveData: false,
      docScore,
      liveScore,
      reason: 'documentation_question',
    };
  }

  if (liveOnlySignals || (shortFollowUp && liveScore > 0)) {
    return {
      mode: 'live',
      needsKnowledge: false,
      needsLiveData: true,
      docScore,
      liveScore,
      reason: 'live_data_question',
    };
  }

  if (docScore > 0) {
    return {
      mode: 'knowledge',
      needsKnowledge: true,
      needsLiveData: false,
      docScore,
      liveScore,
      reason: 'documentation_question',
    };
  }

  if (liveScore > 0) {
    return {
      mode: 'live',
      needsKnowledge: false,
      needsLiveData: true,
      docScore,
      liveScore,
      reason: 'live_data_question',
    };
  }

  return {
    mode: 'neither',
    needsKnowledge: false,
    needsLiveData: false,
    docScore,
    liveScore,
    reason: 'general_conversation',
  };
}

function safeToolError(error) {
  if (error.code === 'STORE_ACCESS_DENIED') return "You don't have access to that store.";
  if (error.code === 'STORE_NOT_FOUND') return 'The selected store could not be found.';
  if (error.code === 'STORE_CONTEXT_REQUIRED') return 'A store must be selected before I can retrieve that data.';
  if (error.code === 'INVALID_TOOL_ARGUMENTS' || error.code === 'INVALID_DATE' || error.code === 'INVALID_DATE_RANGE') {
    return error.message;
  }
  return "I couldn't retrieve that information right now.";
}

function sanitizeAssistantReply(reply) {
  const text = sanitizeAssistantText(reply);
  const cleaned = text
    .replace(/\*\*/g, '')
    .replace(/__/g, '')
    .replace(/`/g, '')
    .replace(/^#{1,6}\s+/gm, '')
    .replace(/^\s*[-*]+\s+/gm, '')
    .replace(/^\s*\d+\.\s+/gm, '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .join(' ');

  return cleaned.replace(/\s+/g, ' ').trim();
}

function buildKnowledgeMessage(query) {
  const chunks = retrieveRelevantKnowledge(query, { limit: MAX_KNOWLEDGE_CHUNKS });
  if (!chunks || chunks.length === 0) {
    return {
      message: {
        role: 'system',
        content: 'No relevant CIVORA documentation was found for this question. If the user asked about CIVORA itself, answer honestly and briefly that the documentation does not cover it.',
      },
      chunks: [],
    };
  }

  return {
    message: formatKnowledgeContext(chunks),
    chunks,
  };
}

function finalizeYuaReply(reply, intent) {
  return formatYuaReply(reply, intent);
}

function buildExtremeMonthReply(monthlyRows, queryText) {
  const extremeMatch = resolveExtremeRow(monthlyRows, queryText);
  if (!extremeMatch) {
    return null;
  }

  return buildExtremeAnswerLines(extremeMatch);
}

async function runToolRounds({ conversation, aiContext, baseMessages, intent }) {
  const agentMessages = [...baseMessages, ...conversation];
  const toolsUsed = [];
  let latestToolResult = null;

  for (let round = 0; round < MAX_TOOL_ROUNDS; round += 1) {
    let assistantMessage;
    try {
      assistantMessage = await createChatCompletion({
        messages: agentMessages,
        tools: toolDefinitions,
      });
    } catch (error) {
      error.stage = error.stage || `tool_round_${round + 1}_llm_call`;
      throw error;
    }

    agentMessages.push({
      role: 'assistant',
      content: assistantMessage.content || '',
      ...(assistantMessage.tool_calls ? { tool_calls: assistantMessage.tool_calls } : {}),
    });

    if (!Array.isArray(assistantMessage.tool_calls) || assistantMessage.tool_calls.length === 0) {
      const reply = sanitizeAssistantReply(assistantMessage.content);
      if (!reply) {
        const error = new Error('CIVORA AI returned an empty response.');
        error.code = 'OLLAMA_MALFORMED_RESPONSE';
        error.stage = `tool_round_${round + 1}_final_response`;
        throw error;
      }

      return {
        reply: finalizeYuaReply(reply, intent),
        tool_rounds: round,
        tools_used: [...new Set(toolsUsed)],
        latest_tool_result: latestToolResult,
      };
    }

    for (const toolCall of assistantMessage.tool_calls) {
      const toolName = toolCall?.function?.name || '';
      let result;
      try {
        result = await executeToolCall(toolCall, { userContext: aiContext });
      } catch (error) {
        console.error(`[AI] Tool ${toolName || 'unknown'} failed: ${error.code || 'ERROR'}`);
        result = { success: false, error: safeToolError(error), code: error.code || 'TOOL_FAILED' };
      }

      latestToolResult = result;
      toolsUsed.push(toolName);
      agentMessages.push({
        role: 'tool',
        tool_call_id: toolCall.id,
        tool_name: toolName,
        content: JSON.stringify(result),
      });
    }

    if (round === MAX_TOOL_ROUNDS - 1) {
      const error = new Error('CIVORA AI reached the tool-call limit.');
      error.code = 'TOOL_ROUND_LIMIT';
      error.stage = 'tool_round_limit';
      throw error;
    }
  }

  throw new Error('CIVORA AI could not complete the request.');
}

async function runYuaAgent({ messages, aiContext, selectionRequired, availableStoreCount }) {
  const conversation = normalizeConversation(messages);
  const latestUserMessage = getLatestUserMessage(conversation);
  const intent = classifyIntent(conversation);
  const baseMessages = [
    { role: 'system', content: getYuaSystemPrompt() },
    buildContextMessage(aiContext, selectionRequired, availableStoreCount),
  ];

  const knowledgeBundle = intent.needsKnowledge
    ? buildKnowledgeMessage(latestUserMessage)
    : { message: null, chunks: [] };

  if (knowledgeBundle.message) {
    baseMessages.push(knowledgeBundle.message);
  }

  console.log('[AI] User request received');
  console.log(`[AI] Intent detected: ${intent.mode}`);
  console.log(`[AI] Knowledge retrieval required: ${Boolean(intent.needsKnowledge)}`);
  console.log(`[AI] Tool call required: ${Boolean(intent.needsLiveData)}`);

  if (isAmbiguousPerformanceQuery(latestUserMessage, conversation)) {
    return {
      reply: finalizeYuaReply('Could you please clarify which performance metric you would like to review (such as revenue, profit, or quantity sold) and which specific month you are referring to?', intent),
      tool_rounds: 0,
      tools_used: [],
      intent,
      knowledge_used: false,
      knowledge_sources: [],
    };
  }

  if (shouldUseForecastReasoning({ queryText: latestUserMessage, intent })) {
    try {
      const result = await runForecastReasoning({
        queryText: latestUserMessage,
        aiContext,
      });

      console.log('[AI] Direct forecast analysis completed');

      let reply = result.reply;
      if (intent.needsKnowledge || /useful|why|how does|what is|purpose|document/i.test(latestUserMessage)) {
        const docSummary = 'According to CIVORA documentation, forecasting uses automated time-series models (Prophet) across 3, 6, and 12-month horizons to help store managers anticipate revenue, plan inventory stock, and evaluate growth or decline trends.';
        reply = `${docSummary}\n\nBased on your store data, here is your current revenue forecast:\n${result.reply}`;
      }

      return {
        ...result,
        reply: finalizeYuaReply(reply, intent),
        intent,
        knowledge_used: true,
        knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
          source_file: chunk.source_file,
          source_name: chunk.source_name,
          page_start: chunk.page_start,
          page_end: chunk.page_end,
          score: chunk.score,
        })),
      };
    } catch (error) {
      if (error.code !== 'FORECAST_NOT_READY') {
        throw error;
      }
    }
  }

  if (shouldUseStoreOverviewReasoning({ queryText: latestUserMessage, conversation, intent })) {
    try {
      const result = await runStoreOverviewReasoning({
        queryText: latestUserMessage,
        aiContext,
      });

      console.log('[AI] Direct store overview completed');

      return {
        ...result,
        intent,
        knowledge_used: knowledgeBundle.chunks.length > 0,
        knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
          source_file: chunk.source_file,
          source_name: chunk.source_name,
          page_start: chunk.page_start,
          page_end: chunk.page_end,
          score: chunk.score,
        })),
      };
    } catch (error) {
      // Fall through to other handlers if needed
    }
  }

  if (shouldUseExtremeReasoning({ queryText: latestUserMessage, intent })) {
    try {
      const result = await runExtremeReasoning({
        queryText: latestUserMessage,
        aiContext,
      });

      console.log('[AI] Direct ranking analysis completed');

      return {
        ...result,
        intent,
        knowledge_used: knowledgeBundle.chunks.length > 0,
        knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
          source_file: chunk.source_file,
          source_name: chunk.source_name,
          page_start: chunk.page_start,
          page_end: chunk.page_end,
          score: chunk.score,
        })),
      };
    } catch (error) {
      if (error.code !== 'EXTREME_PERIOD_NOT_RESOLVED') {
        throw error;
      }
    }
  }

  if (shouldUseAnalyticalReasoning({ queryText: latestUserMessage, conversation, intent })) {
    try {
      const result = await runAnalyticalReasoning({
        queryText: latestUserMessage,
        conversation,
        aiContext,
      });

      console.log('[AI] Direct revenue analysis completed');

      return {
        ...result,
        intent,
        knowledge_used: knowledgeBundle.chunks.length > 0,
        knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
          source_file: chunk.source_file,
          source_name: chunk.source_name,
          page_start: chunk.page_start,
          page_end: chunk.page_end,
          score: chunk.score,
        })),
      };
    } catch (error) {
      if (error.code !== 'ANALYTICAL_PERIOD_NOT_RESOLVED') {
        throw error;
      }
    }
  }

  if (intent.needsLiveData) {
    const result = await runToolRounds({
      conversation,
      aiContext,
      baseMessages,
      intent,
    });

    const comparisonReply = buildRevenueComparisonReply(
      result.latest_tool_result?.monthly_rows,
      latestUserMessage
    );

    if (comparisonReply) {
      return {
        reply: finalizeYuaReply(comparisonReply, { ...intent, mode: 'live', needsLiveData: true }),
        tool_rounds: result.tool_rounds,
        tools_used: result.tools_used,
        intent,
        knowledge_used: knowledgeBundle.chunks.length > 0,
        knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
          source_file: chunk.source_file,
          source_name: chunk.source_name,
          page_start: chunk.page_start,
          page_end: chunk.page_end,
          score: chunk.score,
        })),
      };
    }

    const extremeReply = shouldUseExtremeReasoning({ queryText: latestUserMessage, intent })
      ? buildExtremeMonthReply(result.latest_tool_result?.monthly_rows || [], latestUserMessage)
      : null;

    if (extremeReply) {
      return {
        reply: finalizeYuaReply(extremeReply, { ...intent, mode: 'live', needsLiveData: true }),
        tool_rounds: result.tool_rounds,
        tools_used: result.tools_used,
        intent,
        knowledge_used: knowledgeBundle.chunks.length > 0,
        knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
          source_file: chunk.source_file,
          source_name: chunk.source_name,
          page_start: chunk.page_start,
          page_end: chunk.page_end,
          score: chunk.score,
        })),
      };
    }

    console.log('[AI] Tool completed');
    console.log('[AI] Qwen response generated');

    return {
      ...result,
      intent,
      knowledge_used: knowledgeBundle.chunks.length > 0,
      knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
        source_file: chunk.source_file,
        source_name: chunk.source_name,
        page_start: chunk.page_start,
        page_end: chunk.page_end,
        score: chunk.score,
      })),
    };
  }

  const assistantMessage = await createChatCompletion({
    messages: [...baseMessages, ...conversation],
    tools: [],
  });

  const reply = sanitizeAssistantReply(assistantMessage.content);
  if (!reply) {
    const error = new Error('CIVORA AI returned an empty response.');
    error.code = 'OLLAMA_MALFORMED_RESPONSE';
    error.stage = 'final_synthesis';
    throw error;
  }

  console.log('[AI] Qwen response generated');

  return {
    reply: finalizeYuaReply(reply, intent),
    tool_rounds: 0,
    tools_used: [],
    intent,
    knowledge_used: knowledgeBundle.chunks.length > 0,
    knowledge_sources: knowledgeBundle.chunks.map((chunk) => ({
      source_file: chunk.source_file,
      source_name: chunk.source_name,
      page_start: chunk.page_start,
      page_end: chunk.page_end,
      score: chunk.score,
    })),
  };
}

module.exports = {
  classifyIntent,
  runYuaAgent,
  sanitizeAssistantReply,
};
