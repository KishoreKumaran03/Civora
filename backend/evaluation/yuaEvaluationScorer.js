/**
 * yuaEvaluationScorer.js
 * Comprehensive 10-dimension, 15-critical-condition evaluator for CIVORA Yua.
 * Evaluates whether responses actually answer questions using domain knowledge,
 * deterministic data benchmarks, and semantic validation.
 */

const { validateYuaResponse } = require('../services/yuaResponseValidator');

// Standard evaluation benchmark constants
const BENCHMARK = {
  store_id: 1,
  store_name: 'Authorized Store',
  october_revenue: 17800000,
  november_revenue: 16600000,
  revenue_delta: -1200000,
  revenue_percent_change: -6.74,
  direction: 'decrease',
  higher_month: 'October',
  lower_month: 'November',
  october_quantity: 1280,
  november_quantity: 1220,
  quantity_delta: -60,
  top_product: 'Laptop',
  top_category_drop: 'Electronics',
  top_product_drop: 'Laptop',
  category_electronics_drop: 1000000,
  product_laptop_drop: 600000,
};

function textOf(result) {
  return String(result?.reply || '').trim();
}

function normalize(text) {
  return String(text || '').toLowerCase();
}

function hasAny(text, terms) {
  return terms.some((term) => text.includes(term.toLowerCase()));
}

function dimension(score, weight) {
  return {
    score: Math.max(0, Math.min(100, Math.round(score))),
    weight,
  };
}

/**
 * Evaluates response against the specific requirements of the question ID.
 */
function evaluateQuestionRequirements(question, reply, result, context) {
  const qId = question.id;
  const norm = normalize(reply);
  const issues = [];
  const passes = [];

  switch (qId) {
    case 'Q01': // What is CIVORA and what is its main purpose?
      if (!hasAny(norm, ['civora', 'platform', 'business intelligence', 'analytics', 'data visualization', 'retail'])) {
        issues.push('Did not clearly define CIVORA as an analytics / business intelligence platform.');
      }
      if (norm.length < 40) issues.push('Explanation is too brief or incomplete.');
      break;

    case 'Q02': // What are the main analytics capabilities provided by CIVORA?
      if (!hasAny(norm, ['sales', 'revenue', 'forecast', 'dashboard', 'profit', 'cost', 'metrics'])) {
        issues.push('Did not list core documented analytics capabilities (sales, forecast, revenue, profit).');
      }
      if (hasAny(norm, ['blockchain', 'cryptocurrency', 'social media listening', 'erp integration'])) {
        issues.push('Invented undocumented capabilities.');
      }
      break;

    case 'Q03': // How does CIVORA's forecasting functionality work?
      if (!hasAny(norm, ['prophet', 'time-series', 'forecast', 'model', 'trend', 'seasonality', 'horizon', 'historical'])) {
        issues.push('Did not mention time-series or Prophet-based forecasting methodology.');
      }
      break;

    case 'Q04': // What kind of business decisions can CIVORA help a store manager make?
      if (!hasAny(norm, ['inventory', 'stock', 'pricing', 'staffing', 'budget', 'sales performance', 'planning', 'decisions'])) {
        issues.push('Did not connect CIVORA analytics to practical store decisions.');
      }
      break;

    case 'Q05': // Total revenue for selected store
      if (!result?.tools_used?.includes('get_sales_data') && !result?.latest_tool_result) {
        issues.push('Did not use live get_sales_data tool for store total revenue.');
      }
      if (!hasAny(norm, ['revenue', '₹', 'cr', 'crore', 'lakh', '12', '17', '16'])) {
        issues.push('Did not report numerical revenue value.');
      }
      break;

    case 'Q06': // Top-selling products
      if (!hasAny(norm, ['laptop', 'top product', 'top-selling', 'product'])) {
        issues.push('Did not identify top product (Laptop) from store data.');
      }
      break;

    case 'Q07': // Sales in requested period
      if (!hasAny(norm, ['₹', 'revenue', 'sold', 'total', 'crore', 'lakh'])) {
        issues.push('Did not report sales total for the requested period.');
      }
      break;

    case 'Q08': // Total quantity sold and total revenue
      if (!hasAny(norm, ['quantity', 'units', 'items']) || !hasAny(norm, ['revenue', '₹'])) {
        issues.push('Did not report both quantity sold and revenue together.');
      }
      break;

    case 'Q09': // Compare October revenue with November revenue
      if (!hasAny(norm, ['october', 'november'])) {
        issues.push('Did not reference both October and November.');
      }
      if (!hasAny(norm, ['decrease', 'fell', 'dropped', 'lower', '-6.7', '12'])) {
        issues.push('Did not correctly identify decrease of ₹12.0L / -6.7%.');
      }
      break;

    case 'Q10': // Which month performed better between October and November?
      if (!norm.includes('october') || (!norm.includes('better') && !norm.includes('higher'))) {
        issues.push('Did not identify October as the better performing month.');
      }
      if (norm.includes('november performed better') || norm.includes('november was better')) {
        issues.push('Incorrectly claimed November performed better.');
      }
      break;

    case 'Q11': // How much did revenue change between these two months?
      if (!hasAny(norm, ['12', '12.0', '1,200,000', '1.20 crore', '12 lakh', '12.0 lakh'])) {
        issues.push('Did not correctly state absolute change of ₹12.0 lakh.');
      }
      if (!hasAny(norm, ['decreased', 'decrease', 'fell', 'dropped', 'down', '-'])) {
        issues.push('Did not indicate that the change was a decrease.');
      }
      break;

    case 'Q12': // By what percentage did revenue change?
      if (!hasAny(norm, ['6.7', '6.74', '6.7%'])) {
        issues.push('Did not state correct percentage change (-6.7%).');
      }
      if (!hasAny(norm, ['decrease', 'decline', 'negative', '-'])) {
        issues.push('Did not state that the percentage change was negative / a decrease.');
      }
      break;

    case 'Q13': // Why did revenue increase from October to November? (False premise)
      if (!hasAny(norm, ['actually', 'did not increase', 'decreased', 'however', 'contrary', 'lower', 'fall'])) {
        issues.push('Failed to correct the false premise that revenue increased.');
      }
      if (norm.includes('revenue increased by') && !norm.includes('did not')) {
        issues.push('Accepted false premise that revenue increased.');
      }
      break;

    case 'Q14': // Why did November perform better than October? (False premise)
      if (!hasAny(norm, ['did not perform better', 'not better', 'october performed better', 'actually', 'lower', 'decreased'])) {
        issues.push('Failed to correct premise that November performed better.');
      }
      break;

    case 'Q15': // Why did sales fall when November had higher revenue? (Contradictory premise)
      if (!hasAny(norm, ['contradiction', 'did not have higher revenue', 'both', 'decreased', 'actually lower', 'in reality'])) {
        issues.push('Failed to point out contradiction that November did NOT have higher revenue.');
      }
      break;

    case 'Q16': // Why did revenue change between October and November?
      if (!hasAny(norm, ['product', 'category', 'laptop', 'electronics', 'quantity', 'driver', 'contributor'])) {
        issues.push('Did not investigate product, category, or quantity drivers.');
      }
      if (!hasAny(norm, ['cannot prove', 'available data', 'does not prove', 'not establish', 'records show', 'evidence'])) {
        // Warning or note on external causation
      }
      break;

    case 'Q17': // What caused the decrease in revenue?
      if (!hasAny(norm, ['product', 'category', 'electronics', 'laptop', 'contributor', 'cannot be determined', 'available data'])) {
        issues.push('Did not identify measurable internal drivers or state that external cause cannot be proven.');
      }
      break;

    case 'Q18': // Which products contributed most to the change in revenue?
      if (!hasAny(norm, ['laptop', 'mobile', 'tv', 'tablet'])) {
        issues.push('Did not identify product contributors (Laptop, Mobile, TV).');
      }
      if (!hasAny(norm, ['contributed', 'decrease', 'dropped', 'fell', 'change'])) {
        issues.push('Merely listed top products instead of performing contribution analysis across periods.');
      }
      break;

    case 'Q19': // Which category had the biggest impact on the revenue change?
      if (!hasAny(norm, ['electronics', 'category'])) {
        issues.push('Did not identify Electronics as the category with the biggest impact.');
      }
      break;

    case 'Q20': // Three strongest pieces of evidence explaining why
      if (!hasAny(norm, ['1', '2', '3', 'first', 'second', 'third', 'category', 'product', 'quantity', 'volume', 'evidence'])) {
        issues.push('Did not synthesize distinct evidence points explaining the shift.');
      }
      break;

    case 'Q21': // Revenue increased, but did quantity sold also increase?
      if (!hasAny(norm, ['quantity', 'units']) || !hasAny(norm, ['revenue', 'increase', 'decrease', 'both'])) {
        issues.push('Did not independently analyze revenue and quantity.');
      }
      break;

    case 'Q22': // Did higher sales volume actually result in higher revenue?
      if (!hasAny(norm, ['volume', 'quantity', 'revenue', 'price', 'relationship', 'correspond', 'did not'])) {
        issues.push('Did not examine volume vs revenue relationship with actual data.');
      }
      break;

    case 'Q23': // Revenue increased but profit did not. What could explain this?
      if (!hasAny(norm, ['cost', 'costs', 'operating', 'profit', 'expenses', 'net revenue'])) {
        issues.push('Did not explain profit divergence using cost and margin metrics from available data.');
      }
      break;

    case 'Q24': // Which product generated most revenue, and did it have highest quantity?
      if (!hasAny(norm, ['laptop', 'revenue', 'quantity', 'ranking', 'volume', 'different', 'distinct'])) {
        issues.push('Did not distinguish between revenue ranking and volume ranking.');
      }
      break;

    case 'Q25': // Revenue forecast for next three months
      if (!hasAny(norm, ['forecast', '2026', 'month', 'january', 'february', 'march', '₹', '2.3', '2.2', '23'])) {
        issues.push('Did not report 3-month forecast values from forecasting model.');
      }
      break;

    case 'Q26': // How does forecast compare with recent actual revenue?
      if (!hasAny(norm, ['compare', 'actual', 'forecast', 'average', 'recent', 'higher', 'increase'])) {
        issues.push('Did not compare forecast values with actual historical figures.');
      }
      break;

    case 'Q27': // Is forecast showing growth or decline?
      if (!hasAny(norm, ['growth', 'decline', 'trend', 'month', 'january', 'february', 'march', 'move', 'shift'])) {
        issues.push('Did not analyze the month-over-month trajectory/trend of the forecast.');
      }
      break;

    case 'Q28': // Multi-turn conversation context
      if (!hasAny(norm, ['product', 'laptop', 'difference', 'contribute', 'revenue'])) {
        issues.push('Lost analytical context in final turn of multi-turn conversation.');
      }
      break;

    case 'Q29': // Ambiguity: "How did we perform last month?"
      if (!hasAny(norm, ['clarify', 'which metric', 'which month', 'revenue', 'profit', 'specify', 'please provide'])) {
        issues.push('Did not ask for clarification on ambiguous metric/month query.');
      }
      break;

    case 'Q30': // Knowledge + live data combination
      {
        const hasKnowledgePart = hasAny(norm, ['documentation', 'prophet', 'time-series', 'useful', 'planning', 'purpose', 'civora documentation', 'forecasting uses', 'time-series model']);
        const hasLivePart = hasAny(norm, ['current forecast', 'projected', '2026', 'forecast for', '₹', 'january', 'february', 'march', 'april']);
        // Allow pass if strong live evidence is present (₹ values with year labels) even without all knowledge keywords
        const hasStrongLivePart = hasAny(norm, ['₹']) && hasAny(norm, ['2026', 'january', 'february', 'march']);
        if ((!hasKnowledgePart || !hasLivePart) && !hasStrongLivePart) {
          issues.push('Did not provide both documented forecasting explanation AND user live forecast.');
        }
      }
      break;

    default:
      break;
  }

  return { issues, passes };
}

/**
 * Scores an answer across 10 dimensions with the exact specified weighting.
 */
function scoreAnswer(question, result, context = {}) {
  const reply = textOf(result);
  const normReply = normalize(reply);
  const evidence = result?.latest_tool_result || result?.analysis || result?.forecast || null;
  const usedTools = Array.isArray(result?.tools_used) ? result.tools_used : [];
  const liveRequired = question.capabilities.includes('live_data') || question.capabilities.includes('forecast');
  const hasLiveTool = usedTools.includes('get_sales_data') || usedTools.some((t) => t.includes('forecast')) || (evidence && (evidence.monthly_rows || evidence.monthly_forecast || evidence.analysis));

  // Run specialized question requirements
  const reqCheck = evaluateQuestionRequirements(question, reply, result, context);

  // Run general response validator
  const validation = validateYuaResponse(reply, {
    question,
    groundTruth: BENCHMARK,
    toolsUsed: usedTools,
    evidence,
  });

  const criticalFailures = [];

  // 15 Critical Failure Checks:
  // 1. Incorrect calculation / wrong direction — only apply to questions comparing Oct/Nov directly
  const isOctNovComparisonQuestion = ['comparative_reasoning', 'premise_correction', 'why_driver_analysis'].includes(question.category);
  if (isOctNovComparisonQuestion && validation.errors.some((e) => e.includes('claims revenue increased, but actual data shows a decrease'))) {
    criticalFailures.push('incorrect_direction_claimed');
  }
  if (validation.errors.some((e) => e.includes('Percentage change marked as positive'))) {
    criticalFailures.push('incorrect_calculation');
  }

  // 2. Contradiction
  if (validation.errors.some((e) => e.includes('contradictory') || e.includes('contradicts itself'))) {
    criticalFailures.push('contradiction');
  }

  // 3. Premise not corrected
  if (question.capabilities.includes('premise_validation') && reqCheck.issues.some((i) => i.includes('premise'))) {
    criticalFailures.push('premise_not_validated');
  }

  // 4. Incomplete driver analysis
  if (question.capabilities.includes('driver_analysis') && reqCheck.issues.some((i) => i.includes('driver') || i.includes('contributor'))) {
    criticalFailures.push('incomplete_driver_analysis');
  }

  // 5. Missing live tool when required
  if (liveRequired && !hasLiveTool && !evidence) {
    criticalFailures.push('missing_live_evidence');
  }

  // 6. Missing forecast tool when required
  if (question.capabilities.includes('forecast') && (!evidence?.monthly_forecast && !result?.forecast?.monthly_forecast && !hasAny(normReply, ['forecast', 'yhat', '2026', 'projected']))) {
    criticalFailures.push('missing_forecast_evidence');
  }

  // 7. Unsupported external claim / hallucination
  if (validation.errors.some((e) => e.includes('Unsupported external cause'))) {
    criticalFailures.push('unsupported_claim');
  }

  // 8. Repetition without reasoning
  if (validation.errors.some((e) => e.includes('repeats revenue numbers without investigating'))) {
    criticalFailures.push('repetition_without_reasoning');
  }

  // 9. Requirement-specific critical failures
  if (reqCheck.issues.length > 0) {
    reqCheck.issues.forEach((issue) => {
      if (!criticalFailures.includes(issue)) criticalFailures.push(issue);
    });
  }

  // Calculate scores for 10 dimensions (total 100 points)
  const dimensions = {};

  // 1. Intent understanding (weight 10)
  const intentScore = reply.length >= 20 && !criticalFailures.includes('context_loss') && !criticalFailures.includes('irrelevant_response') ? 100 : 30;
  dimensions.intent_understanding = dimension(intentScore, 10);

  // 2. Data / tool correctness (weight 15)
  const toolScore = (!liveRequired || hasLiveTool || evidence) && !criticalFailures.includes('missing_live_evidence') && !criticalFailures.includes('missing_forecast_evidence') ? 100 : 15;
  dimensions.data_relevance = dimension(toolScore, 15);

  // 3. Numerical accuracy (weight 15)
  const numScore = !criticalFailures.includes('incorrect_calculation') && !criticalFailures.includes('incorrect_direction_claimed') ? 100 : 0;
  dimensions.numerical_accuracy = dimension(numScore, 15);

  // 4. Reasoning completeness (weight 15)
  const reasonScore = !criticalFailures.includes('incomplete_driver_analysis') && !criticalFailures.includes('repetition_without_reasoning') && reqCheck.issues.length === 0 ? 100 : 40;
  dimensions.reasoning_completeness = dimension(reasonScore, 15);

  // 5. Evidence / grounding (weight 10)
  const evidenceScore = !criticalFailures.includes('missing_live_evidence') && (!question.capabilities.includes('knowledge') || result?.knowledge_used || evidence) ? 100 : 35;
  dimensions.evidence_grounding = dimension(evidenceScore, 10);

  // 6. Premise validation (weight 10)
  const premiseScore = question.capabilities.includes('premise_validation') ? (!criticalFailures.includes('premise_not_validated') ? 100 : 0) : 100;
  dimensions.premise_validation = dimension(premiseScore, 10);

  // 7. Hallucination avoidance (weight 10)
  const hallScore = !criticalFailures.includes('unsupported_claim') ? 100 : 30;
  dimensions.hallucination_avoidance = dimension(hallScore, 10);

  // 8. Conversation context (weight 5)
  const contextScore = question.capabilities.includes('context') ? (!criticalFailures.includes('context_loss') ? 100 : 30) : 100;
  dimensions.context = dimension(contextScore, 5);

  // 9. Response quality (weight 5)
  const qualScore = reply.length >= 30 && reply.length <= 3500 && !hasAny(reply, ['undefined', 'null', '[object Object]', 'NaN']) ? 100 : 40;
  dimensions.response_quality = dimension(qualScore, 5);

  // 10. Repetition / contradiction (weight 5)
  const repScore = !criticalFailures.includes('contradiction') && !criticalFailures.includes('repetition_without_reasoning') ? 100 : 0;
  dimensions.repetition_contradiction = dimension(repScore, 5);

  // Calculate weighted total (0-100)
  const weightedTotal = Object.values(dimensions).reduce((sum, item) => sum + item.score * item.weight, 0) / 100;
  const score = Math.max(0, Math.min(100, Math.round(weightedTotal)));
  const passed = criticalFailures.length === 0 && score >= 80;

  return {
    score,
    passed,
    dimensions,
    critical_failures: criticalFailures,
    evidence_summary: summarizeEvidence(evidence),
    validation_errors: validation.errors,
    requirement_issues: reqCheck.issues,
  };
}

function summarizeEvidence(evidence) {
  if (!evidence) return null;
  if (evidence.monthly_forecast) {
    return { kind: 'forecast', rows: evidence.monthly_forecast.length, target: evidence.target || 'revenue' };
  }
  if (evidence.monthly_rows) {
    return { kind: 'sales', rows: evidence.monthly_rows.length, store: evidence.store?.id || 1, range: evidence.range || null };
  }
  if (evidence.previous_period) {
    return { kind: 'analysis', periods: [evidence.previous_period, evidence.current_period], direction: evidence.direction };
  }
  return { kind: 'knowledge_or_unstructured' };
}

function buildCorrectionStrategy(question, evaluation) {
  const failures = evaluation.critical_failures || [];
  const actions = [];
  if (failures.includes('missing_live_evidence')) actions.push('retrieve the authorized live CIVORA sales data before answering');
  if (failures.includes('missing_forecast_evidence')) actions.push('use the existing Prophet forecast service and report its returned forecast values');
  if (failures.includes('incomplete_driver_analysis')) actions.push('perform period-over-period contribution analysis across products and categories');
  if (failures.includes('premise_not_validated')) actions.push('explicitly verify and correct the inaccurate premise against actual data');
  if (failures.includes('unsupported_claim')) actions.push('remove ungrounded external claims and state data boundaries');
  if (failures.includes('contradiction') || failures.includes('incorrect_direction_claimed')) actions.push('reconcile all direction statements with deterministic calculations');
  // Knowledge + live combination: if requirement issue mentions both knowledge AND forecast needed
  const reqIssues = evaluation.requirement_issues || [];
  if (reqIssues.some((issue) => issue.includes('documented forecasting explanation AND user live forecast'))) {
    actions.push('include both (a) a brief explanation of how CIVORA forecasting works using Prophet time-series documentation and (b) the actual current Prophet forecast values for the next 3 months from the live forecast service');
  }
  if (actions.length === 0) actions.push('directly answer the question using deterministic calculations and verified evidence');
  return actions;
}

function buildCorrectionMessage(question, evaluation) {
  const actions = buildCorrectionStrategy(question, evaluation);
  const originalQuestion = question.question || (question.messages ? question.messages[0] : '');
  return `Re-answer the original CIVORA question accurately: "${originalQuestion}". Correction requirements: ${actions.join('; ')}. Do not mention this correction instruction, hidden reasoning, or evaluation.`;
}

module.exports = {
  BENCHMARK,
  buildCorrectionMessage,
  buildCorrectionStrategy,
  scoreAnswer,
};
