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
  "vpcConfig": {
    "vpcId": "<YOUR-VPC-ID>",
    "targetSubnets": ["subnet-12345", "subnet-67890"],
    "securityGroupId": "sg-1234567890abcdef0"
  },
  "deployTestModels": <true/false>
}
```

💡 This file is validated at runtime to ensure all required fields are provided. Deployment will fail if any required fields are missing or invalid.

### Test Models Stack Configuration

The `deployTestModels` flag controls whether to deploy an additional stack containing test model endpoints for development and testing purposes. When set to `true`, this creates:

- **SageMaker Endpoints**: Centerpoint, flood, and multi-container model endpoints
- **HTTP Endpoints**: Test HTTP endpoints for model inference
- **Container Resources**: Model containers and IAM roles
- **VPC Resources**: Dedicated VPC for test models (if not using existing VPC)

**Example configuration:**

```json
{
  "deployTestModels": true
}
```

**Note**: Both the main model runner stack and test models stack share the same VPC. This ensures consistent networking and reduces resource duplication.

### VPC Configuration

The CDK application creates a shared VPC that is used by both the main model runner stack and the test models stack (when enabled). VPC configuration is handled through the `vpcConfig` section in your deployment.json:

- **If `vpcConfig.vpcId` is provided**: Uses the existing VPC with the specified ID
- **If `vpcConfig.vpcId` is not provided**: Creates a new VPC using `ModelRunnerVpc` with sensible defaults:
  - Public and private subnets across 2 availability zones
  - NAT Gateway for private subnet internet access
  - CIDR block: `10.0.0.0/16`

**VPC Configuration Options:**

When using an existing VPC (`vpcConfig.vpcId` provided), you can also specify:

- **`targetSubnets`**: Array of specific subnet IDs to use for test endpoints
- **`securityGroupId`**: Security group ID to use for test endpoints

**Example configurations:**

```json
// Create new VPC with defaults
{
  "projectName": "my-project",
  "account": { "id": "123456789012", "region": "us-west-2", "prodLike": false, "isAdc": false },
  "deployTestModels": true
}

// Use existing VPC with specific subnets and security group
{
  "projectName": "my-project",
  "account": { "id": "123456789012", "region": "us-west-2", "prodLike": false, "isAdc": false },
  "vpcConfig": {
    "vpcId": "vpc-abc123",
    "targetSubnets": ["subnet-12345", "subnet-67890"],
    "securityGroupId": "sg-1234567890abcdef0"
  },
  "deployTestModels": true
}
```

**Benefits of the shared VPC approach:**

- **Resource Efficiency**: Single VPC shared between main and test model stacks reduces resource duplication
- **Consistent Networking**: Both stacks use the same network configuration and security groups
- **Simplified Management**: Single VPC to manage instead of multiple separate VPCs
- **Security**: Private subnets provide additional network isolation for your workloads

This ensures efficient resource usage across both stacks while maintaining proper network isolation.

### Model Runner Dataplane Configuration

The CDK stack demonstrates the Model Runner Dataplane deployment. All configuration is centralized in the `deployment.json` file through the optional `dataplaneConfig` section, which uses the `ModelRunnerDataplaneConfig` type from the local constructs, eliminating the need to modify TypeScript code for customization.

For the complete list of configuration parameters and their defaults, refer to the `ModelRunnerDataplaneConfig` class in `lib/constructs/model-runner-dataplane.ts`.

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

---

## 🧱 Project Structure

```text
cdk
├── bin/
│   ├── app.ts                        # Entry point, loads config and launches stack
│   └── deployment/
│       ├── deployment.json           # Your environment-specific config
│       ├── deployment.json.example   # Template for creating new configs
│       └── load-deployment.ts        # Configuration loader and validator
├── lib/
│   ├── model-runner-stack.ts        # Root CDK stack
│   ├── test-models-stack.ts         # Test models CDK stack
│   └── constructs/                   # Modular construct classes
│       ├── types.ts                  # Common types and interfaces
│       ├── model-runner-dataplane.ts # Main ModelRunnerDataplane construct
│       ├── model-runner-vpc.ts       # ModelRunnerVpc - VPC and networking resources
│       ├── database-tables.ts        # DatabaseTables - DynamoDB tables
│       ├── messaging.ts              # Messaging - SQS queues and SNS topics
│       ├── ecs-service.ts            # ECSService - ECS cluster, services, and roles
│       ├── ecs-roles.ts              # ECSRoles - ECS task and execution roles
│       ├── monitoring.ts             # Monitoring - CloudWatch dashboards
│       ├── autoscaling.ts            # Autoscaling - ECS autoscaling policies
│       └── sinks.ts                  # Sinks - S3 bucket and Kinesis stream outputs
├── test/                             # Unit tests and cdk-nag checks
└── package.json                      # Project config and npm
```

---

## 🏗️ Architecture

This CDK project uses a **modular construct architecture** that separates concerns into focused, reusable classes:

### Core Constructs

- **`types.ts`** - Common interfaces and configuration types
- **`ModelRunnerDataplane`** - Main orchestrator that combines all resources
- **`ModelRunnerVpc`** - Manages VPC creation or import (shared between stacks)
- **`DatabaseTables`** - Manages DynamoDB tables and backup configuration
- **`Messaging`** - Handles SQS queues and SNS topics for async communication
- **`Containers`** - Manages ECS cluster, task definitions, and Fargate services
- **`Monitoring`** - Creates CloudWatch dashboards and monitoring resources
- **`Autoscaling`** - Configures ECS autoscaling policies and alarms
- **`Sinks`** - Manages output sinks (S3 bucket and Kinesis stream)

### Test Models Constructs

When `deployTestModels` is enabled, additional constructs are deployed:

- **`TestEndpoints`** - Main orchestrator for test model endpoints
- **`ModelContainer`** - Container resources for test models
- **`SageMakerRole`** - IAM roles for SageMaker endpoints
- **`CenterpointEndpoint`** - SageMaker endpoint for centerpoint model
- **`FloodEndpoint`** - SageMaker endpoint for flood model
- **`MultiContainerEndpoint`** - Multi-container SageMaker endpoint

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

For deeper hardening guidance, refer to:

- [AWS CDK Security and Safety Dev Guide](https://docs.aws.amazon.com/cdk/v2/guide/security.html)
- Use of [`CliCredentialsStackSynthesizer`](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.CliCredentialsStackSynthesizer.html) for controlling credential use

---

## 🧠 Summary

This CDK project provides infrastructure-as-code for deploying geospatial inference capabilities using AWS native services. It includes security validations via cdk-nag and supports deployment across multiple environments through configuration files.

For questions or contributions, please open an issue or PR.
