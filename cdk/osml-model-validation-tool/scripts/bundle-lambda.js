/**
 * Copyright 2023-2025 Amazon.com, Inc. or its affiliates.
 */

const fs = require('fs-extra');
const path = require('path');

const projectRoot = path.resolve(__dirname, '../../../');
const srcDir = path.join(projectRoot, 'src/aws/osml/model_runner_validation_tool');
const commonDir = path.join(srcDir, 'common');

const outputDir = path.join(__dirname, '../dist');
const lambdaOutputDir = path.join(outputDir, 'lambda');
fs.ensureDirSync(lambdaOutputDir);

console.log('Bundling lambda functions...');
fs.copySync(srcDir, lambdaOutputDir);

console.log('Adding common utilities to lambda functions...');
fs.copySync(commonDir, path.join(lambdaOutputDir, 'aws/osml/model_runner_validation_tool/common'));

// Create __init__.py files to ensure proper Python package structure
const initPaths = [
  path.join(lambdaOutputDir, 'aws'),
  path.join(lambdaOutputDir, 'aws/osml'),
  path.join(lambdaOutputDir, 'aws/osml/model_runner_validation_tool'),
  path.join(lambdaOutputDir, 'aws/osml/model_runner_validation_tool/common')
];
initPaths.forEach(dirPath => {
  fs.ensureDirSync(dirPath);
  const initFilePath = path.join(dirPath, '__init__.py');
  if (!fs.existsSync(initFilePath)) {
    fs.writeFileSync(initFilePath, '');
    console.log(`Created ${initFilePath}`);
  }
});

console.log('Lambda bundling complete!');
