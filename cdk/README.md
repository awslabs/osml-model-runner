# OSML Model Runner – CDK Infrastructure

This CDK project deploys the core infrastructure for running **OSML Model Runner** on AWS.

---

## 📋 Prerequisites

Before deploying, ensure the following tools and resources are available:

- **AWS CLI** configured with credentials
- **AWS CDK CLI** installed (`npm install -g aws-cdk`)
- **Node.js** and **npm** installed
- **Docker** installed and running (for building container images)
- An existing **VPC** with private subnets and NAT Gateway
- An **S3 bucket** for Lambda tool workspace storage

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
    "prodLike: <true/false>,
  },
  "targetVpcId": "<YOUR-VPC-ID>",
  "workspaceBucketName": "<YOUR-WORKSPACE-BUCKET-NAME>"
}
```

💡 This file is validated at runtime to ensure all required fields are provided. Deployment will fail if any required fields are missing or invalid.

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

```
.
├── bin/
│   ├── app.ts                        # Entry point, loads config and launches stack
│   └── deployment/
│       ├── deployment.json           # Your environment-specific config
│       ├── deployment.json.example   # Template for creating new configs
│       └── load-deployment.ts        # Configuration loader and validator
├── lib/
│   ├── osml-model-runner-stack.ts       # Root CDK stack
├── test/                             # Unit tests and cdk-nag checks
└── package.json                      # Project config and npm scripts
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
