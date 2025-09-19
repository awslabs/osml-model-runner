# IAM Permissions Guide for SageMaker Async Endpoints

This guide provides comprehensive information about the IAM permissions required for the SageMaker Async Endpoint integration, including examples for different deployment scenarios.

## Table of Contents

1. [Overview](#overview)
2. [Required Permissions](#required-permissions)
3. [IAM Policy Examples](#iam-policy-examples)
4. [Cross-Account Access](#cross-account-access)
5. [Least Privilege Principles](#least-privilege-principles)
6. [Troubleshooting Permissions](#troubleshooting-permissions)
7. [Security Best Practices](#security-best-practices)

## Overview

The SageMaker Async Endpoint integration requires permissions for several AWS services:

- **Amazon SageMaker**: For invoking async endpoints and checking job status
- **Amazon S3**: For uploading input data and downloading results
- **AWS STS**: For assuming roles (when using cross-account access)

## Required Permissions

### Core SageMaker Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SageMakerAsyncInference",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:*:*:endpoint/*"
            ]
        }
    ]
}
```

### Core S3 Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ObjectOperations",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket/*",
                "arn:aws:s3:::my-async-output-bucket/*"
            ]
        },
        {
            "Sid": "S3BucketOperations",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket",
                "arn:aws:s3:::my-async-output-bucket"
            ]
        }
    ]
}
```

## IAM Policy Examples

### Complete Policy for Single Account

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SageMakerAsyncInference",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:us-west-2:123456789012:endpoint/my-async-endpoint"
            ]
        },
        {
            "Sid": "S3AsyncBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket/*",
                "arn:aws:s3:::my-async-output-bucket/*"
            ]
        },
        {
            "Sid": "S3BucketOperations",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket",
                "arn:aws:s3:::my-async-output-bucket"
            ]
        }
    ]
}
```

### Policy with Wildcard Resources (Development)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SageMakerAsyncInferenceDev",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:*:*:endpoint/dev-*",
                "arn:aws:sagemaker:*:*:endpoint/test-*"
            ]
        },
        {
            "Sid": "S3DevBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::dev-async-*/*",
                "arn:aws:s3:::test-async-*/*"
            ]
        },
        {
            "Sid": "S3DevBucketOperations",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::dev-async-*",
                "arn:aws:s3:::test-async-*"
            ]
        }
    ]
}
```

### Policy with Conditions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SageMakerAsyncInferenceWithConditions",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:*:*:endpoint/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": ["us-west-2", "us-east-1"]
                },
                "DateGreaterThan": {
                    "aws:CurrentTime": "2024-01-01T00:00:00Z"
                }
            }
        },
        {
            "Sid": "S3AccessWithIPRestriction",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket/*",
                "arn:aws:s3:::my-async-output-bucket/*"
            ],
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": ["203.0.113.0/24", "198.51.100.0/24"]
                }
            }
        },
        {
            "Sid": "S3BucketOperationsWithMFA",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-async-input-bucket",
                "arn:aws:s3:::my-async-output-bucket"
            ],
            "Condition": {
                "Bool": {
                    "aws:MultiFactorAuthPresent": "true"
                }
            }
        }
    ]
}
```

## Cross-Account Access

### Scenario 1: SageMaker in Account A, S3 in Account B

#### Account A (SageMaker Account) - Role Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SageMakerAsyncInference",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:us-west-2:111111111111:endpoint/*"
            ]
        },
        {
            "Sid": "AssumeRoleForS3Access",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::222222222222:role/AsyncInferenceS3AccessRole"
        }
    ]
}
```

#### Account B (S3 Account) - Role Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3AsyncBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::account-b-async-input-bucket/*",
                "arn:aws:s3:::account-b-async-output-bucket/*"
            ]
        },
        {
            "Sid": "S3BucketOperations",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::account-b-async-input-bucket",
                "arn:aws:s3:::account-b-async-output-bucket"
            ]
        }
    ]
}
```

#### Account B - Trust Policy for Cross-Account Role

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::111111111111:role/AsyncInferenceExecutionRole"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "unique-external-id-12345"
                }
            }
        }
    ]
}
```

### Scenario 2: Using S3 Bucket Policies for Cross-Account Access

#### S3 Bucket Policy (Account B)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowAsyncInferenceFromAccountA",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::111111111111:role/AsyncInferenceExecutionRole",
                    "arn:aws:iam::111111111111:user/async-inference-user"
                ]
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::account-b-async-bucket/*"
        },
        {
            "Sid": "AllowBucketOperationsFromAccountA",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::111111111111:role/AsyncInferenceExecutionRole",
                    "arn:aws:iam::111111111111:user/async-inference-user"
                ]
            },
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": "arn:aws:s3:::account-b-async-bucket"
        }
    ]
}
```

## Least Privilege Principles

### Environment-Specific Policies

#### Development Environment

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DevSageMakerAccess",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:*:*:endpoint/dev-*"
            ]
        },
        {
            "Sid": "DevS3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::dev-async-*",
                "arn:aws:s3:::dev-async-*/*"
            ]
        }
    ]
}
```

#### Production Environment

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ProdSageMakerAccess",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:us-west-2:123456789012:endpoint/prod-async-endpoint"
            ]
        },
        {
            "Sid": "ProdS3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::prod-async-input-bucket/*",
                "arn:aws:s3:::prod-async-output-bucket/*"
            ]
        },
        {
            "Sid": "ProdS3BucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::prod-async-input-bucket",
                "arn:aws:s3:::prod-async-output-bucket"
            ]
        }
    ]
}
```

### Resource-Specific Policies

#### Time-Based Access

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "BusinessHoursAccess",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": [
                "arn:aws:sagemaker:*:*:endpoint/*"
            ],
            "Condition": {
                "DateGreaterThan": {
                    "aws:CurrentTime": "08:00Z"
                },
                "DateLessThan": {
                    "aws:CurrentTime": "18:00Z"
                }
            }
        }
    ]
}
```

#### Tag-Based Access Control

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "TagBasedSageMakerAccess",
            "Effect": "Allow",
            "Action": [
                "sagemaker:InvokeEndpointAsync"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "sagemaker:ResourceTag/Environment": ["dev", "test"],
                    "sagemaker:ResourceTag/Team": "ml-team"
                }
            }
        },
        {
            "Sid": "TagBasedS3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/Environment": ["dev", "test"],
                    "s3:ExistingObjectTag/Team": "ml-team"
                }
            }
        }
    ]
}
```

## Troubleshooting Permissions

### Common Permission Errors

#### 1. Access Denied for SageMaker Endpoint

**Error Message:**
```
ClientError: An error occurred (AccessDenied) when calling the InvokeEndpointAsync operation: User: arn:aws:iam::123456789012:user/username is not authorized to perform: sagemaker:InvokeEndpointAsync on resource: arn:aws:sagemaker:us-west-2:123456789012:endpoint/my-endpoint
```

**Solution:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sagemaker:InvokeEndpointAsync",
            "Resource": "arn:aws:sagemaker:us-west-2:123456789012:endpoint/my-endpoint"
        }
    ]
}
```

#### 2. S3 Access Denied

**Error Message:**
```
ClientError: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied
```

**Solution:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::my-bucket/*"
        }
    ]
}
```

#### 3. Cross-Account Role Assumption Failed

**Error Message:**
```
ClientError: An error occurred (AccessDenied) when calling the AssumeRole operation: User: arn:aws:iam::111111111111:user/username is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::222222222222:role/CrossAccountRole
```

**Solution:**
Add assume role permission and check trust policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::222222222222:role/CrossAccountRole"
        }
    ]
}
```

### Permission Testing Script

```python
import boto3
from botocore.exceptions import ClientError

def test_permissions(endpoint_name, input_bucket, output_bucket):
    """Test required permissions for async endpoint integration."""
    
    results = {
        "sagemaker_access": False,
        "s3_input_access": False,
        "s3_output_access": False,
        "errors": []
    }
    
    # Test SageMaker access
    try:
        sagemaker_client = boto3.client('sagemaker')
        # This will fail if endpoint doesn't exist, but permission error is different
        sagemaker_client.describe_endpoint(EndpointName=endpoint_name)
        results["sagemaker_access"] = True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ValidationException':
            # Endpoint doesn't exist, but we have permission
            results["sagemaker_access"] = True
        else:
            results["errors"].append(f"SageMaker access error: {e}")
    
    # Test S3 input bucket access
    try:
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=input_bucket)
        results["s3_input_access"] = True
    except ClientError as e:
        results["errors"].append(f"S3 input bucket access error: {e}")
    
    # Test S3 output bucket access
    try:
        s3_client.head_bucket(Bucket=output_bucket)
        results["s3_output_access"] = True
    except ClientError as e:
        results["errors"].append(f"S3 output bucket access error: {e}")
    
    return results

# Usage
results = test_permissions("my-endpoint", "my-input-bucket", "my-output-bucket")
print(f"SageMaker access: {results['sagemaker_access']}")
print(f"S3 input access: {results['s3_input_access']}")
print(f"S3 output access: {results['s3_output_access']}")

if results['errors']:
    print("Errors:")
    for error in results['errors']:
        print(f"  - {error}")
```

## Security Best Practices

### 1. Use IAM Roles Instead of Users

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

### 2. Implement Resource-Based Policies

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "RestrictToSpecificEndpoints",
            "Effect": "Allow",
            "Action": "sagemaker:InvokeEndpointAsync",
            "Resource": [
                "arn:aws:sagemaker:us-west-2:123456789012:endpoint/approved-endpoint-1",
                "arn:aws:sagemaker:us-west-2:123456789012:endpoint/approved-endpoint-2"
            ]
        }
    ]
}
```

### 3. Use Condition Keys for Additional Security

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sagemaker:InvokeEndpointAsync",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "us-west-2"
                },
                "IpAddress": {
                    "aws:SourceIp": "203.0.113.0/24"
                },
                "Bool": {
                    "aws:SecureTransport": "true"
                }
            }
        }
    ]
}
```

### 4. Enable CloudTrail for Audit Logging

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudTrailLogging",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
```

### 5. Implement Temporary Credentials

```python
import boto3
from botocore.exceptions import ClientError

def get_temporary_credentials(role_arn, session_name, duration_seconds=3600):
    """Get temporary credentials for async endpoint access."""
    
    sts_client = boto3.client('sts')
    
    try:
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            DurationSeconds=duration_seconds
        )
        
        credentials = response['Credentials']
        return {
            'AccessKeyId': credentials['AccessKeyId'],
            'SecretAccessKey': credentials['SecretAccessKey'],
            'SessionToken': credentials['SessionToken']
        }
    except ClientError as e:
        print(f"Error assuming role: {e}")
        return None

# Usage
temp_creds = get_temporary_credentials(
    role_arn="arn:aws:iam::123456789012:role/AsyncInferenceRole",
    session_name="async-inference-session"
)

if temp_creds:
    # Use temporary credentials with detector
    detector = AsyncSMDetector(
        endpoint="my-endpoint",
        assumed_credentials=temp_creds,
        async_config=config
    )
```

### 6. Regular Permission Audits

```python
def audit_permissions(role_name):
    """Audit permissions for a specific role."""
    
    iam_client = boto3.client('iam')
    
    try:
        # Get role policies
        response = iam_client.list_attached_role_policies(RoleName=role_name)
        attached_policies = response['AttachedPolicies']
        
        # Get inline policies
        response = iam_client.list_role_policies(RoleName=role_name)
        inline_policies = response['PolicyNames']
        
        print(f"Role: {role_name}")
        print(f"Attached policies: {len(attached_policies)}")
        print(f"Inline policies: {len(inline_policies)}")
        
        # Check for overly broad permissions
        for policy in attached_policies:
            policy_arn = policy['PolicyArn']
            if 'AdministratorAccess' in policy_arn or '*' in policy_arn:
                print(f"WARNING: Broad permissions detected in {policy_arn}")
        
        return {
            "attached_policies": attached_policies,
            "inline_policies": inline_policies
        }
        
    except ClientError as e:
        print(f"Error auditing permissions: {e}")
        return None
```

## Summary

1. **Use Least Privilege**: Grant only the minimum permissions required
2. **Environment Separation**: Use different policies for dev/test/prod
3. **Resource Specificity**: Specify exact resources when possible
4. **Condition Keys**: Use conditions for additional security controls
5. **Regular Audits**: Periodically review and update permissions
6. **Temporary Credentials**: Use temporary credentials when possible
7. **Monitoring**: Enable CloudTrail and monitor access patterns
8. **Cross-Account Security**: Use external IDs and proper trust policies

For additional security guidance, refer to the [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html) documentation.