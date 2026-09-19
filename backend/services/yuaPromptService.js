const fs = require('fs');
const path = require('path');

const DEFAULT_PROMPT_PATH = path.join(__dirname, '..', 'prompts', 'yua-system-prompt.txt');

function resolvePromptPath() {
  const configuredPath = String(process.env.YUA_SYSTEM_PROMPT_PATH || '').trim();
  if (!configuredPath) {
    return DEFAULT_PROMPT_PATH;
  }

  return path.isAbsolute(configuredPath)
    ? configuredPath
    : path.join(__dirname, '..', configuredPath);
}

function getYuaSystemPrompt() {
  const promptPath = resolvePromptPath();
  try {
    return fs.readFileSync(promptPath, 'utf8').trim();
  } catch (_error) {
    return fs.readFileSync(DEFAULT_PROMPT_PATH, 'utf8').trim();
  }
}

module.exports = {
  getYuaSystemPrompt,
  resolvePromptPath,
};
