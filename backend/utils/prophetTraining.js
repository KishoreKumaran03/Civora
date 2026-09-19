const { execFile } = require('child_process');
const path = require('path');

function runProphetRetrain(options = {}) {
  const pythonCommand = process.env.PYTHON_BIN || (process.platform === 'win32' ? 'python' : 'python3');
  const scriptPath = path.join(__dirname, '..', 'scripts', 'train_revenue_prophet.py');
  const pythonArgs = [scriptPath, '--source', options.source || 'database'];

  if (options.projectId != null) {
    pythonArgs.push('--project-id', String(options.projectId));
  }

  if (options.outputDir) {
    pythonArgs.push('--output-dir', String(options.outputDir));
  }

  if (options.forecastDays != null) {
    pythonArgs.push('--forecast-days', String(options.forecastDays));
  }

  if (options.testDays != null) {
    pythonArgs.push('--test-days', String(options.testDays));
  }

  if (options.skipProfit) {
    pythonArgs.push('--skip-profit');
  }

  // Optional: pass a custom promotions workbook path. Defaults are resolved inside the Python script.
  if (options.promotionsWorkbook) {
    pythonArgs.push('--promotions-workbook', String(options.promotionsWorkbook));
  }

  // Optional: pass a custom holidays workbook path.
  if (options.holidaysWorkbook) {
    pythonArgs.push('--holidays-workbook', String(options.holidaysWorkbook));
  }

  return new Promise((resolve, reject) => {
    execFile(pythonCommand, pythonArgs, { env: process.env }, (error, stdout, stderr) => {
      console.log(`Prophet retrain command: ${pythonCommand} ${pythonArgs.join(' ')}`);

      if (stderr) {
        console.error(`Prophet retrain stderr: ${stderr}`);
      }

      if (error) {
        console.error(`Prophet retrain failed: ${error.message}`);
        reject(new Error(error.message));
        return;
      }

      try {
        const payload = JSON.parse(stdout);
        resolve(payload);
      } catch (parseError) {
        reject(new Error(`Invalid Prophet retrain output: ${parseError.message}`));
      }
    });
  });
}

module.exports = {
  runProphetRetrain,
};
