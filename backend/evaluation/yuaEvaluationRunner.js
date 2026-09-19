/**
 * yuaEvaluationRunner.js
 * Automated benchmark evaluation harness for CIVORA Yua reasoning.
 * Executes the 30 reasoning questions, scores responses, runs iterative corrections,
 * saves regression database, and outputs the comprehensive diagnostic report.
 */

const fs = require('fs/promises');
const path = require('path');
const { questions } = require('./yuaReasoningQuestions');
const { installBenchmarkFixture } = require('./yuaEvaluationFixture');
const { resolveCivoraAiContext } = require('../services/civoraAiContext');
const { runYuaAgent } = require('../services/yuaAgentService');
const { buildCorrectionMessage, scoreAnswer } = require('./yuaEvaluationScorer');
const { diagnoseFailure, runIterativeImprovement } = require('../services/yuaEvaluationService');

const MAX_ITERATIONS = 3;
const EVALUATION_DIR = __dirname;
const RESULTS_PATH = path.join(EVALUATION_DIR, 'yuaRegressionResults.json');
const REPORT_PATH = path.join(EVALUATION_DIR, 'yuaEvaluationReport.json');

function parseOptions(argv = process.argv.slice(2)) {
  const options = {
    userId: Number(process.env.EVAL_USER_ID || 10),
    storeId: process.env.EVAL_STORE_ID ? Number(process.env.EVAL_STORE_ID) : 1,
    maxIterations: MAX_ITERATIONS,
  };
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === '--user-id') options.userId = Number(argv[++index]);
    if (argv[index] === '--store-id') options.storeId = Number(argv[++index]);
    if (argv[index] === '--max-iterations') options.maxIterations = Math.max(1, Math.min(MAX_ITERATIONS, Number(argv[++index])));
  }
  return options;
}

function compactTrace(result, evaluation) {
  const evidence = result?.latest_tool_result || result?.analysis || result?.forecast || null;
  return {
    tools_used: result?.tools_used || [],
    tool_rounds: result?.tool_rounds || 0,
    intent: result?.intent?.mode || null,
    knowledge_used: Boolean(result?.knowledge_used),
    evidence: evaluation.evidence_summary,
    structured_reasoning: result?.structured_reasoning || result?.analysis || null,
    forecast_rows: result?.forecast?.monthly_forecast?.length || null,
  };
}

async function runMultiTurn(question, aiContext, context) {
  const conversation = [];
  let lastResult = null;

  for (const userMsg of question.messages) {
    conversation.push({ role: 'user', content: userMsg });
    const result = await runYuaAgent({
      messages: [...conversation],
      aiContext,
      selectionRequired: false,
      availableStoreCount: context.availableStoreCount,
    });
    conversation.push({ role: 'assistant', content: result.reply });
    lastResult = result;
  }

  const evaluation = scoreAnswer(question, lastResult, context);
  return { result: lastResult, evaluation };
}

async function invokeSingle(question, messages, aiContext, context) {
  if (question.multiTurn) {
    return runMultiTurn(question, aiContext, context);
  }

  const result = await runYuaAgent({
    messages,
    aiContext,
    selectionRequired: false,
    availableStoreCount: context.availableStoreCount,
  });
  const evaluation = scoreAnswer(question, result, context);
  return { result, evaluation };
}

async function evaluateQuestion(question, aiContext, context, maxIterations) {
  let messages = question.multiTurn
    ? question.messages.map((content) => ({ role: 'user', content }))
    : [{ role: 'user', content: question.question }];

  const attempts = [];
  let current;

  for (let iteration = 0; iteration < maxIterations; iteration += 1) {
    try {
      if (question.multiTurn && iteration === 0) {
        current = await runMultiTurn(question, aiContext, context);
      } else {
        current = await invokeSingle(question, messages, aiContext, context);
      }
    } catch (error) {
      current = {
        result: { reply: '', tools_used: [], tool_rounds: 0 },
        evaluation: {
          score: 0,
          passed: false,
          dimensions: {},
          critical_failures: [error.code || error.message || 'execution_error'],
          evidence_summary: null,
        },
        error: { message: error.message, code: error.code || null },
      };
    }

    attempts.push({
      iteration: iteration + 1,
      response: current.result.reply || '',
      evaluation: current.evaluation,
      failure_diagnosis: current.evaluation.passed ? null : diagnoseFailure(question, current.evaluation, current.result),
      trace: compactTrace(current.result, current.evaluation),
      error: current.error || null,
    });

    if (current.evaluation.passed) break;

    if (iteration < maxIterations - 1) {
      const correctionInstruction = buildCorrectionMessage(question, current.evaluation);
      messages = [
        ...messages,
        { role: 'assistant', content: current.result.reply || '' },
        { role: 'user', content: correctionInstruction },
      ];
    }
  }

  const first = attempts[0];
  const last = attempts[attempts.length - 1];

  return {
    question_id: question.id,
    question: question.question || question.messages.join(' -> '),
    category: question.category,
    required_capabilities: question.capabilities,
    answer_requirements: question.requirements,
    forbidden_behaviors: question.forbidden,
    initial_response: first.response,
    initial_score: first.evaluation.score,
    failure_reason: first.evaluation.critical_failures,
    failure_diagnosis: first.failure_diagnosis,
    correction_strategy: attempts.length > 1 ? buildCorrectionMessage(question, first.evaluation) : null,
    corrected_response: attempts.length > 1 ? last.response : '',
    corrected_score: attempts.length > 1 ? last.evaluation.score : null,
    iterations: attempts.length,
    final_score: last.evaluation.score,
    passed: last.evaluation.passed,
    evaluation_result: last.evaluation,
    attempts,
    timestamp: new Date().toISOString(),
  };
}

function buildReport(records, options) {
  const firstPass = records.filter((r) => r.attempts[0]?.evaluation?.passed).length;
  const corrected = records.filter((r) => r.attempts.length > 1 && r.passed).length;
  const finalPassed = records.filter((r) => r.passed).length;
  const initialAverage = records.length ? records.reduce((sum, r) => sum + r.initial_score, 0) / records.length : 0;
  const finalAverage = records.length ? records.reduce((sum, r) => sum + r.final_score, 0) / records.length : 0;

  const failureCounts = {};
  records.flatMap((r) => r.failure_reason || []).forEach((failure) => {
    failureCounts[failure] = (failureCounts[failure] || 0) + 1;
  });

  const categoryScores = {};
  records.forEach((r) => {
    if (!categoryScores[r.category]) categoryScores[r.category] = [];
    categoryScores[r.category].push(r.final_score);
  });

  const weakestCapabilities = Object.entries(categoryScores)
    .map(([category, scores]) => ({
      category,
      average: Number((scores.reduce((sum, score) => sum + score, 0) / scores.length).toFixed(1)),
    }))
    .sort((a, b) => a.average - b.average)
    .slice(0, 3);

  const questionsRequiringCorrection = records
    .filter((r) => r.attempts.length > 1)
    .map((r) => ({
      id: r.question_id,
      initial_score: r.initial_score,
      final_score: r.final_score,
      passed: r.passed,
      reason: r.failure_diagnosis?.reason || r.failure_reason?.join(', '),
    }));

  const recommendedArchitectureChanges = [];
  if (weakestCapabilities.some((c) => c.category === 'why_driver_analysis')) {
    recommendedArchitectureChanges.push('Enhance automated multi-dimensional driver attribution before LLM prompt assembly.');
  }
  if (weakestCapabilities.some((c) => c.category === 'conversation_context')) {
    recommendedArchitectureChanges.push('Persist structured analytical context (metric, dates, active store) across turns in session cache.');
  }
  if (weakestCapabilities.some((c) => c.category === 'knowledge_live_combination')) {
    recommendedArchitectureChanges.push('Combine dual-retrieval pipeline explicitly distinguishing documented knowledge from live metrics.');
  }

  return {
    generated_at: new Date().toISOString(),
    options,
    questions: records.length,
    passed_first_attempt: firstPass,
    corrected_after_retry: corrected,
    passed_final: finalPassed,
    failed_after_retries: records.length - finalPassed,
    initial_average: Number(initialAverage.toFixed(1)),
    final_average: Number(finalAverage.toFixed(1)),
    questions_requiring_correction: questionsRequiringCorrection,
    critical_failures: records
      .filter((r) => !r.passed)
      .map((r) => ({ id: r.question_id, failures: r.failure_reason })),
    most_common_failure_types: Object.entries(failureCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([failure, count]) => ({ failure, count })),
    weakest_capabilities: weakestCapabilities,
    recommended_architecture_changes: recommendedArchitectureChanges,
  };
}

async function runEvaluation(options = parseOptions()) {
  installBenchmarkFixture();

  const resolved = await resolveCivoraAiContext({
    userId: options.userId,
    requestContext: options.storeId ? { store_id: options.storeId } : {},
  });

  if (!resolved.aiContext) {
    throw new Error('Evaluation user or store context could not be resolved.');
  }

  const context = {
    userId: options.userId,
    storeId: resolved.aiContext.store_id || 1,
    availableStoreCount: resolved.availableStoreCount || 1,
  };

  const records = [];
  console.log(`Starting CIVORA Yua Reasoning Evaluation (${questions.length} questions)...`);

  for (const question of questions) {
    process.stdout.write(`Evaluating ${question.id} [${question.category}]... `);
    const record = await evaluateQuestion(question, resolved.aiContext, context, options.maxIterations);
    records.push(record);
    process.stdout.write(
      `${record.passed ? 'PASS' : 'FAIL'} (init: ${record.initial_score}, final: ${record.final_score}, iter: ${record.iterations})\n`
    );
  }

  const report = buildReport(records, options);

  await fs.writeFile(RESULTS_PATH, JSON.stringify(records, null, 2), 'utf8');
  await fs.writeFile(REPORT_PATH, JSON.stringify(report, null, 2), 'utf8');

  return { records, report, resultsPath: RESULTS_PATH, reportPath: REPORT_PATH };
}

function printFormattedReport(report) {
  console.log('\n==================================================');
  console.log('CIVORA YUA REASONING EVALUATION');
  console.log('==================================================');
  console.log(`Questions: ${report.questions}`);
  console.log(`Passed first attempt: ${report.passed_first_attempt}`);
  console.log(`Corrected after retry: ${report.corrected_after_retry}`);
  console.log(`Failed after retries: ${report.failed_after_retries}`);
  console.log(`Initial average: ${report.initial_average}`);
  console.log(`Final average: ${report.final_average}`);
  console.log('\nCritical failures:');
  if (report.critical_failures && report.critical_failures.length > 0) {
    report.critical_failures.forEach((item) => {
      console.log(`- ${item.id}: ${(item.failures || []).join(', ')}`);
    });
  } else {
    console.log('- None');
  }

  console.log('\nWeakest capabilities:');
  if (report.weakest_capabilities && report.weakest_capabilities.length > 0) {
    report.weakest_capabilities.forEach((item, index) => {
      console.log(`${index + 1}. ${item.category} (avg score: ${item.average})`);
    });
  } else {
    console.log('- None');
  }
  console.log('==================================================\n');
}

if (require.main === module) {
  runEvaluation()
    .then(({ report }) => {
      printFormattedReport(report);
    })
    .catch((error) => {
      console.error(`Evaluation execution failed: ${error.message}`);
      process.exitCode = 1;
    });
}

module.exports = {
  buildReport,
  evaluateQuestion,
  parseOptions,
  printFormattedReport,
  runEvaluation,
};
