# OSML Model Runner – CDK Infrastructure

This CDK project deploys the core infrastructure for running **OSML Model Runner** on AWS.

---

## 📋 Prerequisites

Before deploying, ensure the following tools and resources are available:

- **AWS CLI** configured with credentials
- **AWS CDK CLI** installed (`npm install -g aws-cdk`)
- **Node.js** and **npm** installed
- **Docker** installed and running (if building container images from source)
- An existing **VPC** with private subnets and NAT Gateway

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
    "prodLike": <true/false>
  },
  "targetVpcId": "<YOUR-VPC-ID>",
}
```

💡 This file is validated at runtime to ensure all required fields are provided. Deployment will fail if any required fields are missing or invalid.

### Model Runner Dataplane Configuration

The CDK stack demonstrates the Model Runner Dataplane deployment. All configuration is centralized in the `deployment.json` file through the optional `mrDataplaneConfig` section, which uses the official `ModelRunnerDataplaneConfig` type from the `osml-cdk-constructs` package, eliminating the need to modify TypeScript code for customization.

For the complete list of configuration parameters and their defaults, refer to the [ModelRunnerDataplaneConfig documentation](https://aws-solutions-library-samples.github.io/osml-cdk-constructs/classes/ModelRunnerDataplaneConfig.html).

#### Example: Custom Configuration

To customize the Dataplane, simply add the `mrDataplaneConfig` section to your `deployment.json` file like the example below:

```json
{
  "mrDataplaneConfig": {
    "ecsTaskCpu": 4096,
    "ecsTaskMemory": 8192,
    "mrEnableMonitoring": true,
    "mrEnableS3Sink": true,
    "cwMetricsNamespace": "MyOSMLProject",
    "mrRegionSize": 2048,
    "mrWorkersPerCpu": 2
  }
}
```

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
│   ├── osml-model-runner-stack.ts   # Root CDK stack
│   └── constructs/                   # Modular construct classes
│       ├── model-runner-dataplane.ts # Main ModelRunnerDataplane construct
│       ├── database-tables.ts        # DatabaseTables - DynamoDB tables
│       ├── messaging.ts              # Messaging - SQS queues and SNS topics
│       ├── containers.ts             # Containers - ECS cluster and services
│       ├── monitoring.ts             # Monitoring - CloudWatch dashboards
│       ├── autoscaling.ts            # Autoscaling - ECS autoscaling policies
│       ├── execution-role.ts         # ExecutionRole - ECS execution role
│       └── task-role.ts              # TaskRole - ECS task role
├── test/                             # Unit tests and cdk-nag checks
└── package.json                      # Project config and npm
```

---

## 🏗️ Architecture

This CDK project uses a **modular construct architecture** that separates concerns into focused, reusable classes:

### Core Constructs

- **`ModelRunnerDataplane`** - Main orchestrator that combines all resources
- **`DatabaseTables`** - Manages DynamoDB tables and backup configuration
- **`Messaging`** - Handles SQS queues and SNS topics for async communication
- **`Containers`** - Manages ECS cluster, task definitions, and Fargate services
- **`Monitoring`** - Creates CloudWatch dashboards and monitoring resources
- **`Autoscaling`** - Configures ECS autoscaling policies and alarms

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
