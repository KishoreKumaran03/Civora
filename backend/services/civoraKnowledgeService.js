const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const DEFAULT_INDEX_PATH = path.join(__dirname, '..', 'data', 'civora_knowledge_index.json');
const DEFAULT_BUILDER_PATH = path.join(__dirname, '..', 'scripts', 'build_civora_knowledge_index.py');
const VECTOR_DIMENSION = 128;
const TOP_K_DEFAULT = 4;

const SOURCE_TOPIC_RULES = [
  {
    keywords: ['dashboard', 'visualization', 'chart', 'charts', 'map', 'graph', 'trend', 'trends'],
    sourceTerms: ['data visualization guide', 'civora platform user manual', 'report generation guide'],
    boost: 0.6,
  },
  {
    keywords: ['sales', 'revenue', 'profit', 'cost', 'margin', 'metric', 'metrics', 'formula', 'calculate'],
    sourceTerms: ['business kpis guide', 'data dictionary', 'civora platform user manual'],
    boost: 0.65,
  },
  {
    keywords: ['forecast', 'predict', 'prediction'],
    sourceTerms: ['forecasting predictive analytics guide', 'ai features guide'],
    boost: 0.65,
  },
  {
    keywords: ['api', 'endpoint', 'authentication', 'token'],
    sourceTerms: ['api documentation', 'administrator guide'],
    boost: 0.6,
  },
  {
    keywords: ['limitation', 'limitations', 'constraint', 'constraints'],
    sourceTerms: ['known limitations system constraints'],
    boost: 0.7,
  },
  {
    keywords: ['civora'],
    sourceTerms: ['civora platform user manual', 'faq', 'ai features guide'],
    boost: 0.18,
  },
];

const STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'how', 'i', 'if', 'in', 'is', 'it', 'its',
  'of', 'on', 'or', 'our', 'that', 'the', 'their', 'them', 'this', 'to', 'was', 'what', 'when', 'where',
  'which', 'who', 'why', 'with', 'you', 'your', 'can', 'could', 'do', 'does', 'did', 'have', 'has', 'had',
  'me', 'my', 'we', 'they', 'there', 'here', 'about', 'into', 'over', 'than', 'then', 'them', 'those', 'these',
  'show', 'tell', 'please', 'help'
]);

let cachedIndex = null;
let loadingPromise = null;

function resolveIndexPath() {
  const configuredPath = String(process.env.YUA_KNOWLEDGE_INDEX_PATH || '').trim();
  if (!configuredPath) {
    return DEFAULT_INDEX_PATH;
  }

  return path.isAbsolute(configuredPath)
    ? configuredPath
    : path.join(__dirname, '..', configuredPath);
}

function resolveBuilderPath() {
  return DEFAULT_BUILDER_PATH;
}

function normalizeText(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(text) {
  return normalizeText(text)
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length > 2 && !STOPWORDS.has(token));
}

function hashToken(token, dimension = VECTOR_DIMENSION) {
  let hash = 2166136261;
  for (let index = 0; index < token.length; index += 1) {
    hash ^= token.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash) % dimension;
}

function buildEmbedding(text, dimension = VECTOR_DIMENSION) {
  const vector = new Array(dimension).fill(0);
  const tokens = tokenize(text);
  if (tokens.length === 0) {
    return vector;
  }

  for (const token of tokens) {
    const slot = hashToken(token, dimension);
    vector[slot] += 1;
  }

  const magnitude = Math.sqrt(vector.reduce((sum, value) => sum + (value * value), 0)) || 1;
  return vector.map((value) => Number((value / magnitude).toFixed(6)));
}

function cosineSimilarity(left, right) {
  if (!Array.isArray(left) || !Array.isArray(right) || left.length === 0 || right.length === 0) {
    return 0;
  }

  const length = Math.min(left.length, right.length);
  let dot = 0;
  let leftMagnitude = 0;
  let rightMagnitude = 0;

  for (let index = 0; index < length; index += 1) {
    const leftValue = Number(left[index] || 0);
    const rightValue = Number(right[index] || 0);
    dot += leftValue * rightValue;
    leftMagnitude += leftValue * leftValue;
    rightMagnitude += rightValue * rightValue;
  }

  if (leftMagnitude === 0 || rightMagnitude === 0) {
    return 0;
  }

  return dot / (Math.sqrt(leftMagnitude) * Math.sqrt(rightMagnitude));
}

function lexicalOverlapScore(queryTokens, chunkTokens) {
  if (!queryTokens.length || !chunkTokens.length) {
    return 0;
  }

  const querySet = new Set(queryTokens);
  const chunkSet = new Set(chunkTokens);
  let matches = 0;

  for (const token of querySet) {
    if (chunkSet.has(token)) {
      matches += 1;
    }
  }

  return matches / Math.max(querySet.size, 1);
}

function sourceTopicBoost(queryTokens, chunk) {
  const sourceText = normalizeText(`${chunk.source_name || ''} ${chunk.title || ''} ${chunk.source_file || ''}`);

  for (const rule of SOURCE_TOPIC_RULES) {
    const keywordHit = rule.keywords.some((keyword) => queryTokens.includes(keyword));
    if (!keywordHit) {
      continue;
    }

    const sourceMatch = rule.sourceTerms.some((term) => sourceText.includes(term));
    if (sourceMatch) {
      return rule.boost;
    }
  }

  return 0;
}

function sharedTokenCount(queryTokens, chunkTokens) {
  if (!queryTokens.length || !chunkTokens.length) {
    return 0;
  }

  const querySet = new Set(queryTokens);
  const chunkSet = new Set(chunkTokens);
  let matches = 0;

  for (const token of querySet) {
    if (chunkSet.has(token)) {
      matches += 1;
    }
  }

  return matches;
}

function ensureKnowledgeIndexBuilt() {
  const indexPath = resolveIndexPath();
  if (fs.existsSync(indexPath)) {
    return indexPath;
  }

  const builderPath = resolveBuilderPath();
  const pythonCommand = process.env.PYTHON_BIN || (process.platform === 'win32' ? 'python' : 'python3');
  execFileSync(pythonCommand, [builderPath, indexPath], {
    stdio: 'inherit',
    cwd: path.join(__dirname, '..'),
  });

  if (!fs.existsSync(indexPath)) {
    const error = new Error('CIVORA knowledge index could not be generated.');
    error.code = 'KNOWLEDGE_INDEX_MISSING';
    throw error;
  }

  return indexPath;
}

function loadKnowledgeIndex() {
  if (cachedIndex) {
    return cachedIndex;
  }

  const indexPath = ensureKnowledgeIndexBuilt();
  const raw = fs.readFileSync(indexPath, 'utf8');
  const parsed = JSON.parse(raw);

  cachedIndex = {
    ...parsed,
    chunks: Array.isArray(parsed.chunks) ? parsed.chunks : [],
    dimension: Number(parsed.dimension || VECTOR_DIMENSION),
  };

  return cachedIndex;
}

function getKnowledgeChunks() {
  const index = loadKnowledgeIndex();
  return index.chunks || [];
}

function retrieveRelevantKnowledge(query, { limit = TOP_K_DEFAULT } = {}) {
  const chunks = getKnowledgeChunks();
  const queryTokens = tokenize(query);
  const queryEmbedding = buildEmbedding(query, VECTOR_DIMENSION);
  const normalizedQuery = normalizeText(query);
  const requiredOverlap = queryTokens.length <= 1 ? 1 : Math.min(2, queryTokens.length);

  const scoredChunks = chunks
    .map((chunk) => {
      const chunkTokens = Array.isArray(chunk.tokens) ? chunk.tokens : tokenize(chunk.text || '');
      const embedding = Array.isArray(chunk.embedding) ? chunk.embedding : buildEmbedding(chunk.text || '', VECTOR_DIMENSION);
      const chunkText = normalizeText(chunk.text || '');
      const semanticScore = cosineSimilarity(queryEmbedding, embedding);
      const lexicalScore = lexicalOverlapScore(queryTokens, chunkTokens);
      const overlapCount = sharedTokenCount(queryTokens, chunkTokens);
      const titleBoost = tokenize(chunk.title || '').some((token) => queryTokens.includes(token)) ? 1 : 0;
      const phraseBoost = normalizedQuery && chunkText.includes(normalizedQuery) ? 1 : 0;
      const topicBoost = sourceTopicBoost(queryTokens, chunk);
      const score = Number((lexicalScore * 0.72 + semanticScore * 0.14 + titleBoost + phraseBoost + topicBoost).toFixed(6));

      return {
        ...chunk,
        tokens: chunkTokens,
        overlap_count: overlapCount,
        title_boost: titleBoost,
        phrase_boost: phraseBoost,
        topic_boost: topicBoost,
        score,
      };
    })
    .filter((chunk) => chunk.overlap_count >= requiredOverlap || chunk.title_boost > 0 || chunk.phrase_boost > 0 || chunk.topic_boost > 0)
    .sort((left, right) => right.score - left.score)
    .slice(0, Math.max(1, limit));

  return scoredChunks;
}

function formatKnowledgeContext(chunks) {
  if (!Array.isArray(chunks) || chunks.length === 0) {
    return null;
  }

  const lines = ['Relevant CIVORA documentation:'];

  for (const chunk of chunks) {
    const source = chunk.source_name || chunk.source_file || 'documentation';
    const pageInfo = chunk.page_start != null
      ? `page ${chunk.page_start}${chunk.page_end != null && chunk.page_end !== chunk.page_start ? `-${chunk.page_end}` : ''}`
      : 'document';
    const text = String(chunk.text || '').replace(/\s+/g, ' ').trim();
    lines.push(`${source} (${pageInfo}): ${text}`);
  }

  return {
    role: 'system',
    content: lines.join('\n'),
  };
}

module.exports = {
  buildEmbedding,
  formatKnowledgeContext,
  getKnowledgeChunks,
  loadKnowledgeIndex,
  normalizeText,
  retrieveRelevantKnowledge,
  tokenize,
};
