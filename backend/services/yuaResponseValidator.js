/**
 * yuaResponseValidator.js
 * Comprehensive validation engine for CIVORA Yua responses.
 * Detects contradictions, numerical errors, unsupported claims/hallucinations,
 * uncorrected premises, and repetitive answers without reasoning.
 */

const UNSUPPORTED_EXTERNAL_CAUSES = [
  'inflation',
  'macroeconomic',
  'competitor',
  'weather',
  'climate',
  'seasonal holiday spike',
  'economic downturn',
  'supply chain crisis',
  'consumer sentiment survey',
  'viral marketing campaign',
  'store renovation',
];

function normalizeText(text) {
  return String(text || '').toLowerCase();
}

/**
 * Detects contradictions within an answer or against ground truth.
 */
function detectContradictions(text, groundTruth = null) {
  const norm = normalizeText(text);
  const issues = [];
  const sanitizedForCheck = norm
    .replace(/\b(growth\s+(or|and|\/)\s*decline|increase\s+(or|and|\/)\s*decrease|increases\s+(or|and|\/)\s*decreases|rise\s+(or|and|\/)\s*fall|project\s+growth\s+or\s+decline|projected\s+growth\s+or\s+decline)\b/g, 'trend_variation');

  // Check simultaneous increase & decrease claims without qualifying contrast
  const hasIncrease = /\b(increased|growth|grew|rise|rose|went up)\b/.test(sanitizedForCheck);
  const hasDecrease = /\b(decreased|decline|declined|fell|dropped|went down)\b/.test(sanitizedForCheck);

  if (hasIncrease && hasDecrease) {
    // Legitimate if contrasting premise or different metrics/periods
    // Also acceptable if the response is correcting a false premise ("revenue increased" in question, "decreased" in answer)
    const hasContrast = /\b(actually|however|instead|contrary|while|whereas|not|rather than|did not|does not|in fact|the premise|no,|incorrect|trend|relative to|compared to|evaluating|forecast)\b/.test(norm);
    // Acceptable if correcting a premise: question may have said "increased" but answer correctly states "decreased"
    const isPremiseCorrection = /\b(did not increase|revenue did not|did not grow|the premise|actually decreased|actually shows a decrease|in fact.*decreas)\b/.test(norm);
    if (!hasContrast && !isPremiseCorrection) {
      issues.push('Response makes contradictory statements about direction without qualifying context.');
    }
  }

  // Check specific contradiction: "decreased" but says "November was higher" when November was lower
  if (norm.includes('decreased') && norm.includes('november was higher')) {
    issues.push('Response contradicts itself by stating revenue decreased while claiming November was higher.');
  }

  if (norm.includes('increased') && norm.includes('october was higher')) {
    issues.push('Response contradicts itself by stating revenue increased while claiming October was higher.');
  }

  // Check against ground truth direction if supplied
  if (groundTruth && groundTruth.direction) {
    if (groundTruth.direction === 'decrease') {
      if (/\brevenue (increased|grew|rose)\b/.test(norm) && !/\b(not|didn't|did not|actually decreased|contrary)\b/.test(norm)) {
        issues.push(`Response claims revenue increased, but actual data shows a decrease.`);
      }
      if (/\bnovember was (the )?higher\b/.test(norm)) {
        issues.push('Response claims November was higher, but actual data shows October was higher.');
      }
    } else if (groundTruth.direction === 'increase') {
      if (/\brevenue (decreased|declined|fell|dropped)\b/.test(norm) && !/\b(not|didn't|did not|actually increased|contrary)\b/.test(norm)) {
        issues.push(`Response claims revenue decreased, but actual data shows an increase.`);
      }
    }
  }

  return issues;
}

/**
 * Validates numerical consistency with ground truth values.
 */
function validateNumericalConsistency(text, groundTruth) {
  const issues = [];
  if (!groundTruth) return issues;

  const norm = normalizeText(text);

  // Check percentage change direction
  if (groundTruth.percentage_change != null) {
    const isNegative = groundTruth.percentage_change < 0;
    if (isNegative && (norm.includes('+') && norm.includes(Math.abs(groundTruth.percentage_change).toFixed(1)))) {
      issues.push('Percentage change marked as positive (+) when it is negative.');
    }
  }

  return issues;
}

/**
 * Detects unsupported external claims or business hallucinations.
 */
function detectHallucinations(text, allowedEvidence = {}) {
  const norm = normalizeText(text);
  const issues = [];

  for (const cause of UNSUPPORTED_EXTERNAL_CAUSES) {
    if (norm.includes(cause)) {
      // Allowed only if explicitly acknowledged as unproven/uncertainty statement
      const isUncertainty = /\b(cannot prove|not enough evidence|available data does not|unsupported|not recorded|cannot establish|does not prove)\b/.test(norm);
      if (!isUncertainty) {
        issues.push(`Unsupported external cause asserted without data evidence: "${cause}".`);
      }
    }
  }

  return issues;
}

/**
 * Validates whether a false premise in the user question was correctly validated and corrected.
 */
function validatePremiseCorrection(text, questionMeta) {
  const issues = [];
  if (!questionMeta || !questionMeta.capabilities || !questionMeta.capabilities.includes('premise_validation')) {
    return issues;
  }

  const norm = normalizeText(text);
  const corrected = /\b(actually|however|did not|does not show|contrary|premise|in reality|data shows a decrease|data shows that november was lower|not higher|not accurate|not correct|not an increase|not perform better|lower revenue|performed better|october performed better|october 2024|the data actually|october had higher|october generated more)\b/.test(norm);

  if (!corrected) {
    issues.push('Question contained an inaccurate premise, but the response failed to verify or correct it.');
  }

  return issues;
}

/**
 * Detects if a response merely repeats raw revenue numbers without performing driver or causal analysis.
 */
function detectRepetitionWithoutReasoning(text, questionMeta) {
  const issues = [];
  if (!questionMeta) return issues;

  const isWhyOrDriverQuestion = questionMeta.category === 'why_driver_analysis' ||
    (questionMeta.capabilities && questionMeta.capabilities.includes('driver_analysis'));

  if (!isWhyOrDriverQuestion) return issues;

  const norm = normalizeText(text);
  // Extended driver pattern: cost/expense/margin/profit analysis is a valid driver investigation for profit-divergence questions
  const hasDrivers = /\b(product|category|region|quantity|unit|contributor|driven|driver|drop in|shift|breakdown|evidence|cannot establish|cannot be determined|impact|strongest|cost|costs|expense|expenses|margin|net revenue|net profit|operating|total cost|grew|compresses|granular)\b/.test(norm);

  if (!hasDrivers) {
    issues.push('Response repeats revenue numbers without investigating or reporting measurable drivers or stating data limitations.');
  }

  return issues;
}

/**
 * Master validation function for a Yua response.
 */
function validateYuaResponse(reply, { question = {}, groundTruth = null, toolsUsed = [], evidence = null } = {}) {
  const text = String(reply || '').trim();
  const errors = [];
  const warnings = [];

  if (!text) {
    errors.push('Empty response produced.');
    return { valid: false, errors, warnings };
  }

  // 1. Contradictions
  const contradictions = detectContradictions(text, groundTruth);
  errors.push(...contradictions);

  // 2. Numerical consistency
  const numericalIssues = validateNumericalConsistency(text, groundTruth);
  errors.push(...numericalIssues);

  // 3. Hallucinations
  const hallucinations = detectHallucinations(text, evidence);
  errors.push(...hallucinations);

  // 4. Premise validation
  const premiseIssues = validatePremiseCorrection(text, question);
  errors.push(...premiseIssues);

  // 5. Repetition without reasoning
  const repetitionIssues = detectRepetitionWithoutReasoning(text, question);
  errors.push(...repetitionIssues);

  // 6. Live data vs Knowledge tool selection check
  const liveRequired = question.capabilities && (question.capabilities.includes('live_data') || question.capabilities.includes('forecast'));
  const hasLiveTool = toolsUsed.includes('get_sales_data') || toolsUsed.some((t) => t.includes('forecast')) || (evidence && (evidence.monthly_rows || evidence.monthly_forecast || evidence.analysis));
  if (liveRequired && !hasLiveTool) {
    errors.push('Live store data or forecast required by question, but no live data tool was executed.');
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

module.exports = {
  detectContradictions,
  detectHallucinations,
  detectRepetitionWithoutReasoning,
  validateNumericalConsistency,
  validatePremiseCorrection,
  validateYuaResponse,
};
