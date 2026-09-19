/**
 * yuaEvaluationService.js
 * Core evaluation, diagnostic, and iterative correction service for CIVORA Yua.
 * Coordinates failure diagnosis, reasoning reformulation, and iterative re-evaluation.
 */

const { scoreAnswer } = require('../evaluation/yuaEvaluationScorer');
const { validateYuaResponse } = require('./yuaResponseValidator');
const { runAnalyticalReasoning } = require('./yuaAnalyticsReasoningService');
const { runForecastReasoning } = require('./yuaForecastReasoningService');
const { getSalesData } = require('./civoraAiService');
const { createChatCompletion } = require('./llmService');

const MAX_CORRECTION_ITERATIONS = 3;

/**
 * Diagnoses the specific failure type, reason, and missing capability.
 */
function diagnoseFailure(question, evaluation, result = {}) {
  const critical = evaluation.critical_failures || [];
  const dims = evaluation.dimensions || {};
  const reply = String(result?.reply || '').toLowerCase();

  // 1. Contradiction
  if (critical.includes('contradiction') || (dims.repetition_contradiction && dims.repetition_contradiction.score < 50)) {
    return {
      failure_type: 'contradiction',
      reason: 'Response contains mutually contradictory statements about performance direction or outcome.',
      missing_capability: 'contradiction_detection',
      required_actions: ['Reconcile direction statements with ground truth data', 'Ensure single consistent conclusion'],
    };
  }

  // 2. Premise unvalidated
  if (critical.includes('premise_not_validated') || (dims.premise_validation && dims.premise_validation.score < 50)) {
    return {
      failure_type: 'premise_uncorrected',
      reason: 'Question contained an inaccurate premise which was accepted without verification.',
      missing_capability: 'premise_validation',
      required_actions: ['Verify direction before answering', 'Explicitly correct inaccurate premise', 'Report actual metrics'],
    };
  }

  // 3. Driver analysis incomplete
  if (critical.includes('incomplete_driver_analysis') || (dims.reasoning_completeness && dims.reasoning_completeness.score < 50)) {
    return {
      failure_type: 'incomplete_reasoning',
      reason: 'Assistant retrieved high-level totals but did not investigate product, category, or volume contributors.',
      missing_capability: 'driver_analysis',
      required_actions: ['Perform period-over-period contribution analysis', 'Investigate product and category shifts', 'Report measurable drivers'],
    };
  }

  // 4. Missing live evidence / tool selection
  if (critical.includes('missing_live_evidence') || (dims.data_relevance && dims.data_relevance.score < 50)) {
    return {
      failure_type: 'tool_selection_error',
      reason: 'Question requires live store data or forecast, but tool was not executed or evidence was not retrieved.',
      missing_capability: 'live_data',
      required_actions: ['Execute get_sales_data or get_revenue_forecast tool', 'Ground answer in live store records'],
    };
  }

  // 5. Missing forecast evidence
  if (critical.includes('missing_forecast_evidence')) {
    return {
      failure_type: 'missing_forecast_evidence',
      reason: 'Forecasting question answered without Prophet model forecast values.',
      missing_capability: 'forecast',
      required_actions: ['Execute get_revenue_forecast service', 'Report Prophet projection rows'],
    };
  }

  // 6. Hallucination / Unsupported claim
  if (critical.includes('unsupported_claim') || (dims.hallucination_avoidance && dims.hallucination_avoidance.score < 50)) {
    return {
      failure_type: 'hallucination',
      reason: 'Assistant asserted external market, competitor, or economic causes not supported by CIVORA data.',
      missing_capability: 'hallucination_prevention',
      required_actions: ['Remove unsupported external claims', 'Explicitly state that available data cannot establish external cause'],
    };
  }

  // 7. Numerical accuracy
  if (critical.includes('incorrect_calculation') || (dims.numerical_accuracy && dims.numerical_accuracy.score < 50)) {
    return {
      failure_type: 'numerical_inaccuracy',
      reason: 'Calculated difference, percentage, or reported totals do not match deterministic truth.',
      missing_capability: 'numerical_accuracy',
      required_actions: ['Recalculate differences and percentages deterministically', 'Verify against store records'],
    };
  }

  return {
    failure_type: 'incomplete_answer',
    reason: 'The response does not fully satisfy all requirements for this analytical competency.',
    missing_capability: question.capabilities ? question.capabilities[0] : 'reasoning',
    required_actions: ['Answer the question directly using retrieved evidence and deterministic calculations'],
  };
}

/**
 * Builds the reformulated reasoning context based on diagnosed failure.
 */
async function buildImprovedReasoning({ question, failureDiagnosis, aiContext, conversation = [] }) {
  const queryText = question.question || (question.messages ? question.messages[question.messages.length - 1] : '');
  const capabilities = question.capabilities || [];

  // If driver analysis or comparison is needed, run backend deterministic reasoning
  if (capabilities.includes('driver_analysis') || capabilities.includes('comparison') || capabilities.includes('premise_validation')) {
    try {
      const analyticalResult = await runAnalyticalReasoning({
        queryText,
        conversation,
        aiContext,
      });
      return {
        reply: analyticalResult.reply,
        analysis: analyticalResult.analysis,
        structured_reasoning: analyticalResult.structured_reasoning,
        tools_used: ['get_sales_data'],
        mode: 'deterministic_analytics',
      };
    } catch (err) {
      // fallback
    }
  }

  // If forecast is needed
  if (capabilities.includes('forecast')) {
    try {
      const forecastResult = await runForecastReasoning({
        queryText,
        aiContext,
      });
      return {
        reply: forecastResult.reply,
        forecast: forecastResult.forecast,
        tools_used: forecastResult.tools_used,
        mode: 'forecast',
      };
    } catch (err) {
      // fallback
    }
  }

  // If live data retrieval needed
  if (capabilities.includes('live_data')) {
    try {
      const salesData = await getSalesData({
        userContext: aiContext,
        storeId: aiContext?.store_id || null,
      });
      return {
        reply: null,
        latest_tool_result: salesData,
        tools_used: ['get_sales_data'],
        mode: 'live_data',
      };
    } catch (err) {
      // fallback
    }
  }

  return null;
}

/**
 * Orchestrates the full iterative improvement loop for a question.
 */
async function runIterativeImprovement({
  question,
  initialResult,
  initialEvaluation,
  aiContext,
  context,
  maxIterations = MAX_CORRECTION_ITERATIONS,
}) {
  const attempts = [
    {
      iteration: 1,
      response: initialResult.reply || '',
      evaluation: initialEvaluation,
      failure_diagnosis: initialEvaluation.passed ? null : diagnoseFailure(question, initialEvaluation, initialResult),
      tools_used: initialResult.tools_used || [],
      trace: initialResult.structured_reasoning || null,
    },
  ];

  if (initialEvaluation.passed) {
    return {
      passed: true,
      iterations: 1,
      attempts,
      final_score: initialEvaluation.score,
      final_response: initialResult.reply,
      final_evaluation: initialEvaluation,
    };
  }

  let currentResult = initialResult;
  let currentEvaluation = initialEvaluation;

  for (let iteration = 2; iteration <= maxIterations; iteration += 1) {
    const diagnosis = diagnoseFailure(question, currentEvaluation, currentResult);
    const improvedEvidence = await buildImprovedReasoning({
      question,
      failureDiagnosis: diagnosis,
      aiContext,
      conversation: question.multiTurn ? question.messages.map((m) => ({ role: 'user', content: m })) : [],
    });

    let correctedReply = '';
    if (improvedEvidence && improvedEvidence.reply) {
      correctedReply = improvedEvidence.reply;
    } else {
      // Synthesize with Qwen using the specific diagnostic requirements
      const prompt = `You are CIVORA Yua. Correct your previous response to: "${question.question}".
Failure Reason: ${diagnosis.reason}
Required Actions: ${diagnosis.required_actions.join('; ')}
${improvedEvidence?.latest_tool_result ? `Evidence: ${JSON.stringify(improvedEvidence.latest_tool_result.totals)}` : ''}
Provide a concise, direct, accurate answer without mentioning these instructions or evaluation.`;

      try {
        const completion = await createChatCompletion({
          messages: [{ role: 'user', content: prompt }],
          tools: [],
        });
        correctedReply = String(completion?.content || '').trim();
      } catch (err) {
        correctedReply = improvedEvidence?.reply || currentResult.reply || '';
      }
    }

    const correctedResult = {
      ...currentResult,
      reply: correctedReply,
      tools_used: improvedEvidence?.tools_used || currentResult.tools_used || [],
      latest_tool_result: improvedEvidence?.latest_tool_result || currentResult.latest_tool_result,
      analysis: improvedEvidence?.analysis || currentResult.analysis,
      structured_reasoning: improvedEvidence?.structured_reasoning || currentResult.structured_reasoning,
      forecast: improvedEvidence?.forecast || currentResult.forecast,
    };

    const newEvaluation = scoreAnswer(question, correctedResult, context);

    attempts.push({
      iteration,
      response: correctedReply,
      evaluation: newEvaluation,
      failure_diagnosis: newEvaluation.passed ? null : diagnoseFailure(question, newEvaluation, correctedResult),
      tools_used: correctedResult.tools_used,
      trace: correctedResult.structured_reasoning || null,
    });

    currentResult = correctedResult;
    currentEvaluation = newEvaluation;

    if (newEvaluation.passed) {
      break;
    }
  }

  const lastAttempt = attempts[attempts.length - 1];
  return {
    passed: lastAttempt.evaluation.passed,
    iterations: attempts.length,
    attempts,
    final_score: lastAttempt.evaluation.score,
    final_response: lastAttempt.response,
    final_evaluation: lastAttempt.evaluation,
  };
}

module.exports = {
  diagnoseFailure,
  runIterativeImprovement,
};
