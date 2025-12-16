# OSML Model Runner – CDK Infrastructure

This CDK project deploys the core infrastructure for running **OSML Model Runner** on AWS.

---

## 📋 Prerequisites

Before deploying, ensure the following tools and resources are available:

- **AWS CLI** configured with credentials
- **AWS CDK CLI** installed (`npm install -g aws-cdk`)
- **Node.js** and **npm** installed
- **Docker** installed and running (if building container images from source)
- An existing **VPC** with private subnets and NAT Gateway (optional - a new VPC with proper networking will be created automatically if not specified)

---

## 🏗️ Stacks Overview

This CDK application deploys multiple stacks that work together to provide the complete Model Runner infrastructure. Understanding what each stack creates will help you plan your deployment and manage resources effectively.

### NetworkStack (`<project-name>-Network`)

The **NetworkStack** provides the foundational networking infrastructure that all other stacks depend on. It manages VPC creation or import and network security.

**Resources Created:**

- **VPC**: Either creates a new VPC or imports an existing one based on configuration
  - New VPCs include public and private subnets across 2 availability zones
  - CIDR block: `10.0.0.0/16` (configurable)
  - Internet Gateway for public subnets
  - NAT Gateway for private subnet egress
- **Security Groups**: Network security rules for Model Runner services
- **VPC Flow Logs**: Network traffic logging (enabled for production-like environments)
- **Subnet Selection**: Configures which subnets to use for resource deployment

**Key Features:**

- Can import existing VPCs to integrate with your existing network infrastructure
- Supports custom subnet and security group selection
- Automatically configures proper routing for public and private subnets

---

### ModelRunnerStack (`<project-name>-Dataplane`)

The **ModelRunnerStack** deploys the core Model Runner dataplane - the main application infrastructure that processes geospatial imagery and runs ML models.

**Resources Created:**

#### **Database (DynamoDB)**

- **Outstanding Image Requests Table**: Tracks pending image processing requests
- **Image Request Table**: Stores image request status and metadata
- **Features Table**: Stores extracted feature data from processed images
- **Endpoint Statistics Table**: Tracks endpoint performance metrics
- **Region Request Table**: Manages region-level processing requests
- **AWS Backup Configuration**: Automated backups for production environments (when `prodLike: true`)

#### **Messaging (SQS & SNS)**

- **Image Request Queue**: Primary queue for image processing requests
- **Region Request Queue**: Queue for region-level processing tasks
- **Dead Letter Queues**: Handles failed messages from both request queues
- **Image Status Topic** (optional): SNS topic for image processing status notifications
- **Region Status Topic** (optional): SNS topic for region processing status notifications
- **Status Queues** (optional): SQS queues subscribed to status topics

#### **Compute (ECS)**

- **ECS Cluster**: Container orchestration cluster for Model Runner tasks
- **Fargate Service**: Serverless container service running Model Runner containers
- **Task Definition**: Container configuration with CPU, memory, and environment variables
- **Container Image**: Model Runner Docker image (built from source or pulled from registry)
- **CloudWatch Log Group**: Centralized logging for all container logs
- **ECS Roles**: IAM roles for task execution and task permissions

#### **Autoscaling**

- **ECS Service Autoscaler**: Automatically scales ECS tasks based on queue depth
- **CloudWatch Alarms**: Monitors queue metrics to trigger scaling events
- **Scaling Policies**: Configurable min/max task counts and scaling increments

#### **Output Sinks**

- **S3 Bucket** (optional): Stores processed output data and results
  - Includes access logging bucket
  - Versioning enabled for production environments
  - Server-side encryption enabled
- **Kinesis Data Stream** (optional): Real-time streaming output for processed data

#### **Monitoring**

- **CloudWatch Dashboard** (optional): Pre-configured dashboard with key metrics
  - ECS service metrics (CPU, memory, task count)
  - Queue metrics (message count, visibility timeout)
  - Custom Model Runner metrics

**Key Features:**

- Fully serverless architecture using Fargate (no EC2 instances to manage)
- Automatic scaling based on workload
- Configurable resource allocation (CPU, memory, worker count)
- Optional status notifications via SNS
- Production-ready with backups, encryption, and monitoring

---

### IntegrationTestStack (`<project-name>-IntegrationTest`)

The **IntegrationTestStack** deploys test infrastructure for development and integration testing. This stack is only deployed when `deployIntegrationTests: true` in your configuration.

**Resources Created:**

#### **Test Imagery**

- **S3 Bucket**: Dedicated bucket for storing test imagery files
- **Bucket Deployment**: Automatically uploads test images from local `cdk/assets/imagery/` directory
- **Encryption**: Server-side encryption enabled
- **Access Control**: Private bucket with proper IAM permissions

#### **Test Models**

- **SageMaker Endpoints**:
  - **Centerpoint Endpoint**: Object detection model endpoint
  - **Flood Endpoint**: Flood detection model endpoint
  - **Failure Endpoint**: Error scenario testing model endpoint
  - **Multi-Container Endpoint**: Multi-model inference endpoint
- **HTTP Endpoint**: Container-based HTTP endpoint for testing HTTP model integration
- **Container Resources**: ECS task definitions and services for HTTP endpoint
- **IAM Roles**: SageMaker execution roles with necessary permissions

**Key Features:**

- Provides ready-to-use test models for validating Model Runner functionality
- Includes both SageMaker and HTTP-based endpoints for different integration patterns
- Test imagery is automatically deployed for immediate use
- Shares the same VPC as ModelRunnerStack for consistent networking

---

### SageMakerRoleStack (`<project-name>-SageMakerRole`)

The **SageMakerRoleStack** creates a dedicated IAM role for SageMaker endpoints. This is deployed separately to ensure proper cleanup of network interfaces when endpoints are deleted.

**Resources Created:**

- **IAM Role**: SageMaker execution role with permissions for:
  - Accessing S3 buckets
  - CloudWatch logging
  - VPC network access
  - Model artifact access

**Note**: This stack exists as a workaround for a CloudFormation limitation with SageMaker endpoint cleanup. It ensures network interfaces are properly cleaned up when endpoints are deleted.

---

## ⚙️ Configuration

### Deployment File: `bin/deployment/deployment.json`

This file defines your deployment environment. Copy the example file and customize it:

```bash
cp bin/deployment/deployment.json.example bin/deployment/deployment.json
```

Update the contents:

```json
{
  "projectName": "<YOUR-PROJECT-NAME>",
  "account": {
    "id": "<YOUR-ACCOUNT-ID>",
    "region": "<YOUR-REGION>",
    "prodLike": <true/false>,
    "isAdc": <true/false>
  },
  "networkConfig": {
    "vpcId": "<YOUR-VPC-ID>",
    "targetSubnets": ["subnet-12345", "subnet-67890"],
    "securityGroupId": "sg-1234567890abcdef0"
  },
  "deployIntegrationTests": <true/false>
}
```

💡 This file is validated at runtime to ensure all required fields are provided. Deployment will fail if any required fields are missing or invalid.

### Integration Tests Configuration

The `deployIntegrationTests` flag controls whether to deploy additional stacks containing integration test infrastructure for development and testing purposes. When set to `true`, this creates:

- **Test Models Stack**: SageMaker endpoints, HTTP endpoints, and container resources for test models
- **Test Imagery Stack**: S3 bucket and image deployment for test imagery
- **Shared VPC**: Both stacks share the same VPC as the main model runner stack for consistent networking

**Example configuration:**

```json
{
  "deployIntegrationTests": true
}
```

**Note**: The integration test stacks share the same VPC as the main model runner stack. This ensures consistent networking and reduces resource duplication.

### VPC Configuration

The CDK application creates a shared VPC that is used by both the main model runner stack and the test models stack (when enabled). VPC configuration is handled through the `networkConfig` section in your deployment.json:

- **If `networkConfig.vpcId` is provided**: Uses the existing VPC with the specified ID
- **If `networkConfig.vpcId` is not provided**: Creates a new VPC using `Network` with sensible defaults:
  - Public and private subnets across 2 availability zones
  - NAT Gateway for private subnet internet access
  - CIDR block: `10.0.0.0/16`

**VPC Configuration Options:**

When using an existing VPC (`networkConfig.vpcId` provided), you can also specify:

- **`targetSubnets`**: Array of specific subnet IDs to use for test endpoints
- **`securityGroupId`**: Security group ID to use for test endpoints

**Example configurations:**

Create new VPC with defaults:

```json
{
  "projectName": "my-project",
  "account": {
    "id": "123456789012",
    "region": "us-west-2",
    "prodLike": false
  },
  "deployIntegrationTests": true
}
```

Import an existing VPC with specific subnets and security group:

```json
{
  "projectName": "my-project",
  "account": {
    "id": "123456789012",
    "region": "us-west-2",
    "prodLike": false,
    "isAdc": false
  },
  "networkConfig": {
    "vpcId": "vpc-abc123",
    "targetSubnets": ["subnet-12345", "subnet-67890"],
    "securityGroupId": "sg-1234567890abcdef0"
  },
  "deployIntegrationTests": true
}
```

**Benefits of the shared VPC approach:**

- **Resource Efficiency**: Single VPC shared between main and test model stacks reduces resource duplication
- **Consistent Network**: Both stacks use the same network configuration and security groups
- **Simplified Management**: Single VPC to manage instead of multiple separate VPCs
- **Security**: Private subnets provide additional network isolation for your workloads

This ensures efficient resource usage across both stacks while maintaining proper network isolation.

### Model Runner Dataplane Configuration

The CDK stack demonstrates the Model Runner Dataplane deployment. All configuration is centralized in the `deployment.json` file through the optional `dataplaneConfig` section, which uses the `DataplaneConfig` type from the local constructs, eliminating the need to modify TypeScript code for customization.

For the complete list of configuration parameters and their defaults, refer to the `DataplaneConfig` class in `lib/constructs/model-runner/dataplane.ts`.

#### Example: Custom Configuration

To customize the Dataplane, simply add the `dataplaneConfig` section to your `deployment.json` file like the example below:

```json
{
  "dataplaneConfig": {
    "ECS_TASK_CPU": 4096,
    "ECS_TASK_MEMORY": 8192,
    "MR_ENABLE_MONITORING": true,
    "MR_ENABLE_S3_SINK": true,
    "CW_METRICS_NAMESPACE": "MyOSMLProject",
    "MR_REGION_SIZE": 2048,
    "MR_WORKERS_PER_CPU": 2
  }
}
```

#### Building Containers from Source

By default, the CDK uses the pre-built container image from the registry. To build the container from source instead, set `BUILD_FROM_SOURCE: true` in your configuration:

```json
{
  "dataplaneConfig": {
    "BUILD_FROM_SOURCE": true
  }
}
```

**Note**: When building from source, ensure Docker is installed and running on your deployment machine. The build process will use the Dockerfile.model-runner in the docker directory and target the `model_runner` stage of the multi-stage build.

---

## 🚀 Deployment Instructions

### 1. Install Dependencies

```bash
npm install
```

### 2. Synthesize the Stack

```bash
cdk synth
```

### 3. Deploy the Stack

```bash
cdk deploy
```

This command will:

- Validate `deployment.json`
- Synthesize the CloudFormation template
- Deploy the infrastructure to your AWS account

**Note**: CDK will display the changes that will be made and prompt you to approve them before proceeding with the deployment. Review the changes carefully and type `y` to confirm the deployment.

#### Automated Deployment

For automated deployments or CI/CD pipelines, we recommend using:

```bash
cdk deploy --all --require-approval never --concurrency 3
```

This command will:

- Deploy all stacks in the application
- Skip interactive approval prompts
- Automatically proceed with deployment changes
- Deploy multiple stacks in parallel (up to 3 concurrent deployments)

---

## 🏗️ Architecture

This CDK project uses a **modular construct architecture** that separates concerns into focused, reusable classes:

### Core Constructs

- **`types.ts`** - Common interfaces and configuration types
- **`ModelRunnerDataplane`** - Main orchestrator that combines all resources
- **`Network`** - Manages VPC creation or import (shared between stacks)
- **`DatabaseTables`** - Manages DynamoDB tables and backup configuration
- **`Messaging`** - Handles SQS queues and SNS topics for async communication
- **`Containers`** - Manages ECS cluster, task definitions, and Fargate services
- **`Monitoring`** - Creates CloudWatch dashboards and monitoring resources
- **`Autoscaling`** - Configures ECS autoscaling policies and alarms
- **`Sinks`** - Manages output sinks (S3 bucket and Kinesis stream)

### Test Models Constructs

When `deployTestModels` is enabled, additional constructs are deployed:

- **`TestModels`** - Main orchestrator for test model endpoints
- **`ModelContainer`** - Container resources for test models
- **`SageMakerRole`** - IAM roles for SageMaker endpoints
- **`CenterpointEndpoint`** - SageMaker endpoint for centerpoint model

When `deployTestImagery` is enabled, additional constructs are deployed:

- **`TestImagery`** - S3 bucket and image deployment for test imagery

### Benefits

- **Modularity**: Each construct has a single, clear responsibility
- **Reusability**: Constructs can be used independently or in other projects
- **Maintainability**: Easier to debug, test, and modify specific functionality
- **Type Safety**: Full TypeScript support with proper interfaces

### Usage Example

```typescript
// Access specific resources through the main dataplane
const dataplane = new ModelRunnerDataplane(this, "MRDataplane", { ... });

// Direct access to resource groups
const tables = dataplane.databaseTables;
const queues = dataplane.messaging;
const services = dataplane.containers;
const sinks = dataplane.sinks;
```

---

## 🧪 Development & Testing

### Useful Commands

| Command         | Description                                          |
| --------------- | ---------------------------------------------------- |
| `npm run build` | Compile TypeScript to JavaScript                     |
| `npm run watch` | Auto-recompile on file changes                       |
| `npm run test`  | Run Jest unit tests                                  |
| `cdk synth`     | Generate CloudFormation template                     |
| `cdk diff`      | Compare local stack with deployed version            |
| `cdk deploy`    | Deploy the CDK stack                                 |
| `cdk destroy`   | Remove the deployed stack                            |
| `cdk bootstrap` | Bootstrap CDK in your AWS account (first-time setup) |
| `cdk list`      | List all stacks in the app                           |

---

## 🔐 Security & Best Practices

This project integrates **cdk-nag** to validate infrastructure against AWS security best practices. Running `npm run test` will:

- Detect overly permissive IAM roles and security groups
- Ensure encryption is enabled where applicable
- Warn about missing logging or compliance settings

📄 **Review the cdk-nag report** to maintain compliance and security posture before production deployments.

### CDK-NAG Report Generation

The test suite automatically generates comprehensive cdk-nag compliance reports during test execution. The reporting system works as follows:

#### How Reports Are Generated

1. **During Test Execution**: Each stack test (`model-runner-stack.test.ts`, `network-stack.test.ts`, etc.) runs cdk-nag's `AwsSolutionsChecks` and calls `generateNagReport()` which:
   - Extracts errors and warnings from stack annotations
   - Collects suppressed violations from stack template metadata
   - Displays a formatted compliance report to stdout
   - Aggregates suppressed violations for the final report

2. **After All Tests Complete**: The Jest global teardown hook (configured in `jest.config.js`) automatically calls `generateFinalSuppressedViolationsReport()`, which:
   - Consolidates all suppressed violations from all test stacks
   - Generates a comprehensive report file: `cdk-nag-suppressions-report.txt`
   - Includes summary statistics by rule type and detailed breakdowns by stack

#### Report Files

After running tests, you'll find:

- **`cdk-nag-suppressions-report.txt`**: Comprehensive report of all suppressed NAG violations across all stacks
  - Summary by rule type showing violation counts
  - Detailed breakdown per stack with resource-level information
  - Suppression reasons for each violation

#### Viewing Reports

```bash
# Run tests to generate reports
npm run test

# View the final suppressed violations report
cat cdk-nag-suppressions-report.txt
```

#### Understanding Suppressions

The report distinguishes between:

- **Errors**: Unsuppressed violations that need to be fixed
- **Warnings**: Unsuppressed warnings that should be reviewed
- **Suppressed Violations**: Violations that have been explicitly suppressed with documented reasons

Each suppressed violation includes:

- The NAG rule that was suppressed (e.g., `AwsSolutions-S1`)
- The resource where the suppression applies
- The reason for suppression (as documented in the code)

For deeper hardening guidance, refer to:

- [AWS CDK Security and Safety Dev Guide](https://docs.aws.amazon.com/cdk/v2/guide/security.html)
- Use of [`CliCredentialsStackSynthesizer`](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.CliCredentialsStackSynthesizer.html) for controlling credential use

---

## 🧠 Summary

This CDK project provides infrastructure-as-code for deploying geospatial inference capabilities using AWS native services. It includes security validations via cdk-nag and supports deployment across multiple environments through configuration files.

For questions or contributions, please open an issue or PR.
