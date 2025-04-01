# OSML Model Validation Tool

This CDK project deploys an infrastructure for validating machine learning models for compatibility with SageMaker and Oversight Machine Learning, as well as benchmarking model performance.

## Architecture

The Model Validation Tool consists of the following components:

1. **SNS Topics** for event-driven communication:
   - `ModelValidationInvokeRequestTopic`: Receives validation requests
   - `ModelValidationSuccessTopic`: Publishes successful validation notifications
   - `ModelValidationFailureTopic`: Publishes failed validation notifications

2. **Step Functions Workflow** that orchestrates the validation process:
   - SageMaker Compatibility Test (required)
   - Oversight ML Compatibility Test (optional)
   - Benchmarking Tasks (optional)
     - Performance Benchmarking
     - SageMaker Inference Recommender
   - Report Generation

3. **Lambda Functions** for validation tasks:
   - `SageMakerCompatibilityLambda`: Tests model compatibility with SageMaker
   - `OversightMLCompatibilityLambda`: Tests model compatibility with Oversight ML
   - `CompileReportLambda`: Compiles validation results into a comprehensive report

4. **ECS Fargate Tasks** for resource-intensive operations:
   - Benchmarking Task: Runs performance benchmarks on the model
   - Inference Recommender Task: Calls SageMaker Inference Recommender API

5. **S3 Buckets** for storing results:
   - Report Bucket: Stores validation reports

6. **EventBridge Rule** to trigger the Step Functions workflow from SNS messages

## Deployment

### Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js and npm installed
- AWS CDK installed (`npm install -g aws-cdk`)

### Deploy the Stack

1. Install dependencies:
   ```
   npm install
   ```

2. Build the TypeScript code:
   ```
   npm run build
   ```

3. Deploy the stack:
   ```
   npx cdk deploy
   ```

## Usage

### Option 1: Using AWS CLI to send a validation request via SNS

```bash
aws sns publish \
  --topic-arn <ModelValidationInvokeRequestTopicArn> \
  --message '{
    "modelInfo": {
      "modelName": "my-model",
      "modelArn": "arn:aws:sagemaker:us-west-2:123456789012:model/my-model",
      "modelId": "my-model-id",
      "modelVersion": "1.0",
      "modelDescription": "My test model",
      "modelOwner": "John Doe",
      "modelCreationDate": "2023-01-01T00:00:00Z"
    },
    "runOversightMLCompatibilityTest": true,
    "runBenchmarkingTask": true,
    "runInferenceRecommender": true
  }'
```

### Option 2: Starting the Step Functions workflow directly

```bash
aws stepfunctions start-execution \
  --state-machine-arn <StateMachineArn> \
  --input '{
    "modelInfo": {
      "modelName": "my-model",
      "modelArn": "arn:aws:sagemaker:us-west-2:123456789012:model/my-model",
      "modelId": "my-model-id",
      "modelVersion": "1.0",
      "modelDescription": "My test model",
      "modelOwner": "John Doe",
      "modelCreationDate": "2023-01-01T00:00:00Z"
    },
    "runOversightMLCompatibilityTest": true,
    "runBenchmarkingTask": true,
    "runInferenceRecommender": true
  }'
```

### Option 3: Using the AWS Management Console

1. Navigate to the Step Functions console
2. Find the Model Validation workflow
3. Click "Start execution"
4. Enter the input JSON with model information and test configuration
5. Click "Start execution"

## Customization

You can customize the stack by modifying the `bin/osml-model-validation-tool.ts` file:

```typescript
new OsmlModelValidationToolStack(app, 'OsmlModelValidationToolStack', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
  reportBucketName: 'my-custom-report-bucket',
  logLevel: 'DEBUG', // Set logging level: DEBUG, INFO, WARNING, ERROR, or CRITICAL
});
```

## Testing

Run the unit tests:

```bash
npm test
```

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npx cdk deploy`  deploy this stack to your default AWS account/region
* `npx cdk diff`    compare deployed stack with current state
* `npx cdk synth`   emits the synthesized CloudFormation template
