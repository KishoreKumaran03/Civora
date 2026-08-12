const { execFile } = require('child_process');
const path = require('path');

function buildPythonArgs(mode, filePath, options = {}) {
  const pythonArgs = [path.join(__dirname, '..', 'scripts', 'process_excel.py'), mode, filePath];

  if (mode === 'process') {
    pythonArgs.push(
      String(options.projectId ?? ''),
      String(options.month ?? ''),
      String(options.year ?? ''),
      String(options.userId ?? ''),
      JSON.stringify(options.columnMapping || {})
    );
  }

  return pythonArgs;
}

function runPythonUploadTask(mode, filePath, options = {}) {
  const pythonCommand = process.env.PYTHON_BIN || (process.platform === 'win32' ? 'python' : 'python3');
  const pythonArgs = buildPythonArgs(mode, filePath, options);

  return new Promise((resolve, reject) => {
    execFile(pythonCommand, pythonArgs, (error, stdout, stderr) => {
      console.log(`Python Command: ${pythonCommand} ${pythonArgs.join(' ')}`);

      if (stderr) {
        console.error(`Python Stderr: ${stderr}`);
      }

      if (error) {
        console.error(`Exec Error: ${error.message}`);
        reject(new Error(error.message));
        return;
      }

      console.log(`Python Stdout: ${stdout}`);

      try {
        resolve(JSON.parse(stdout));
      } catch (parseError) {
        console.error(`JSON Parse Error: ${parseError.message}. Raw output: ${stdout}`);
        reject(new Error('Invalid output from processing script'));
      }
    });
  });
}

module.exports = { runPythonUploadTask };
