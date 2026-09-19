const { runYuaAgent } = require('./yuaAgentService');

async function runCivoraAgent(options) {
  return runYuaAgent(options);
}

module.exports = {
  runCivoraAgent,
};
