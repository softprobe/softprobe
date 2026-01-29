# AWS Benchmark Guide

Complete guide for running full stress performance benchmarks on AWS with EC2 instances, S3 storage, and Iceberg REST catalog.

## Prerequisites

### 1. AWS CLI Setup

#### Install AWS CLI

**macOS:**
```bash
brew install awscli
```

**Linux:**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

**Windows:**
Download from https://aws.amazon.com/cli/

#### Configure AWS Credentials

```bash
aws configure
```

You'll be prompted for:
- **AWS Access Key ID**: Your access key
- **AWS Secret Access Key**: Your secret key
- **Default region**: e.g., `us-east-1`
- **Default output format**: `json` (recommended)

#### Verify Configuration

```bash
aws sts get-caller-identity
```

This should return your AWS account ID, user ARN, and user ID. If this fails, your credentials are not configured correctly.

### 2. Required IAM Permissions

As a super admin, you have full access. For reference, the script requires these minimum permissions:

**EC2:**
- `ec2:RunInstances` - Launch EC2 instances
- `ec2:TerminateInstances` - Terminate instances
- `ec2:DescribeInstances` - List instance details
- `ec2:DescribeVpcs` - Get VPC information
- `ec2:DescribeSubnets` - Get subnet information
- `ec2:DescribeImages` - Find AMI images
- `ec2:CreateSecurityGroup` - Create security groups
- `ec2:DeleteSecurityGroup` - Delete security groups
- `ec2:AuthorizeSecurityGroupIngress` - Configure security group rules
- `ec2:AuthorizeSecurityGroupEgress` - Configure security group rules
- `ec2:DescribeSecurityGroups` - List security groups
- `ec2:CreateKeyPair` / `ec2:ImportKeyPair` - Manage SSH keys
- `ec2:DescribeKeyPairs` - List SSH keys

**S3:**
- `s3:CreateBucket` - Create S3 buckets
- `s3:DeleteBucket` - Delete buckets
- `s3:ListBucket` - List bucket contents
- `s3:GetObject` - Read objects
- `s3:PutObject` - Write objects
- `s3:DeleteObject` - Delete objects

**IAM:**
- `iam:CreateRole` - Create IAM roles
- `iam:DeleteRole` - Delete roles
- `iam:CreateInstanceProfile` - Create instance profiles
- `iam:DeleteInstanceProfile` - Delete instance profiles
- `iam:PutRolePolicy` - Attach policies
- `iam:DeleteRolePolicy` - Remove policies
- `iam:AddRoleToInstanceProfile` - Attach roles to profiles
- `iam:RemoveRoleFromInstanceProfile` - Detach roles from profiles
- `iam:GetRole` - Read role details
- `iam:GetInstanceProfile` - Read profile details

**STS:**
- `sts:GetCallerIdentity` - Verify credentials

### 3. SSH Key Pair

The script will automatically create an SSH key pair named `datalake-benchmark-key` if it doesn't exist. The private key will be saved to `~/.ssh/datalake-benchmark-key.pem`.

**Important:** Save this key file - you'll need it to SSH to instances for debugging.

## Quick Start

### 1. Setup Stack

```bash
./scripts/aws_benchmark.sh setup
```

This will:
- Create S3 bucket for warehouse storage
- Launch two EC2 instances (c5.2xlarge each)
- Deploy REST catalog (Lakekeeper) on instance 1
- Deploy application on instance 2
- Configure IAM roles for S3 access
- Save state to `aws_benchmark_state.json`

**Expected time:** 5-10 minutes

### 2. Run Benchmark

```bash
# Default parameters (300s duration, 200 QPS each)
./scripts/aws_benchmark.sh run

# Custom parameters
./scripts/aws_benchmark.sh run \
    --duration 600 \
    --span-qps 500 \
    --log-qps 500 \
    --metric-qps 500 \
    --query-concurrency 32 \
    --query-interval-ms 300
```

**Parameters:**
- `--duration`: Test duration in seconds (default: 300)
- `--span-qps`: Spans per second to ingest (default: 200)
- `--log-qps`: Logs per second to ingest (default: 200)
- `--metric-qps`: Metrics per second to ingest (default: 200)
- `--query-concurrency`: Concurrent query workers (default: 16)
- `--query-interval-ms`: Interval between queries per worker (default: 500)

### 3. Cleanup

```bash
./scripts/aws_benchmark.sh cleanup
```

This will:
- Terminate both EC2 instances
- Delete S3 bucket and all contents
- Remove security groups
- Clean up IAM roles and policies
- Remove state file

**Important:** Always run cleanup to avoid ongoing charges!

## Architecture

```
┌─────────────────┐         ┌─────────────────┐
│  EC2 Instance 1 │         │  EC2 Instance 2 │
│  (REST Catalog) │         │   (Application) │
│                 │         │                  │
│  - PostgreSQL   │         │  - Rust App      │
│  - Lakekeeper   │◄────────┤  - perf_stress  │
│  (port 8181)    │  HTTP   │  - DuckDB        │
└─────────────────┘         └────────┬────────┘
                                      │
                                      │ S3 API
                                      ▼
                              ┌───────────────┐
                              │  S3 Bucket    │
                              │  (warehouse)  │
                              └───────────────┘
```

### Instance 1: REST Catalog (Lakekeeper)
- **Purpose**: Iceberg REST catalog service
- **Services**: PostgreSQL + Lakekeeper (Docker containers)
- **Port**: 8181 (catalog API)
- **Storage**: PostgreSQL database (local)

### Instance 2: Application
- **Purpose**: Run benchmark tests
- **Services**: Rust application, DuckDB
- **Storage**: Local cache directory (`/var/tmp/datalake/cache`)
- **Network**: Connects to Instance 1 for catalog, S3 for storage

### S3 Bucket
- **Purpose**: Iceberg warehouse storage
- **Structure**: `s3://bucket-name/warehouse/`
- **Access**: Via IAM role (no hardcoded keys)

## Configuration

### Environment Variables

```bash
# Instance type (default: c5.2xlarge)
export INSTANCE_TYPE=c5.4xlarge

# AWS region (default: us-east-1)
export AWS_REGION=us-west-2

# Bucket name prefix (default: datalake-benchmark)
export BUCKET_PREFIX=my-benchmark

# SSH key name (default: datalake-benchmark-key)
export KEY_NAME=my-key
```

### Custom AMI

If you need a custom AMI:

```bash
export AMI_ID=ami-0123456789abcdef0
./scripts/aws_benchmark.sh setup
```

### Application Code Deployment

The script expects the application code to be available on the instance. Options:

**Option A: Public Git Repository**
- Update `deploy_application()` in script to clone from your repo

**Option B: Copy via SCP**
- Manually copy code to instance after setup:
```bash
scp -i ~/.ssh/datalake-benchmark-key.pem -r . ubuntu@APP_IP:~/datalake
```

**Option C: Pre-built Docker Image**
- Build Docker image, push to ECR, pull on instance

## Understanding Results

The benchmark reports:

1. **Ingestion Stats**:
   - Records produced per type (spans/logs/metrics)
   - Ingestion errors

2. **Query Performance**:
   - Total queries executed
   - Query errors
   - **Steady-state latency** (post-warmup):
     - Average latency
     - P95 latency
   - **Per-query-type breakdown**:
     - Execution count
     - Error count
     - Average latency
     - P95 latency

### Target Metrics

Based on design goals:
- **Business attribute lookups**: <500ms p95
- **Error rate queries**: <1s p95
- **Latency analysis**: <2s p95
- **Session drilldowns**: <1s p95

## Monitoring

### During Benchmark

1. **CloudWatch**: Monitor EC2 metrics (CPU, memory, network)
2. **S3 Console**: Monitor bucket size and request metrics
3. **SSH Access**: Connect to instances for debugging:
```bash
ssh -i ~/.ssh/datalake-benchmark-key.pem ubuntu@INSTANCE_IP
```

### Cost Monitoring

**EC2 Costs** (us-east-1):
- c5.2xlarge: ~$0.34/hour per instance
- Two instances: ~$0.68/hour
- 1-hour benchmark: ~$0.68

**S3 Costs**:
- Storage: $0.023/GB/month
- PUT requests: $0.005 per 1,000
- GET requests: $0.0004 per 1,000
- Data transfer out: $0.09/GB (first 10TB)

**Estimated Total**: ~$1-2 for a 1-hour benchmark (excluding data transfer)

## Troubleshooting

### Setup Fails

**Error: "AWS credentials not configured"**
```bash
aws configure
aws sts get-caller-identity  # Verify
```

**Error: "No default VPC found"**
- Create a default VPC or specify a custom VPC in the script

**Error: "SSH not ready"**
- Check security group allows SSH (port 22) from your IP
- Verify instance is running: `aws ec2 describe-instances --instance-ids i-xxx`

### Benchmark Fails

**Error: "Application code not found"**
- Copy code to instance manually (see Application Code Deployment above)

**Error: "REST catalog not accessible"**
- Check catalog instance is running
- Verify security group allows port 8181 from app instance
- SSH to catalog instance and check logs: `sudo docker compose logs -f`

**High Query Latency**
- Check CloudWatch metrics for CPU/memory
- Verify S3 bucket is in same region as instances
- Check network bandwidth limits

### Cleanup Fails

**Error: "Bucket not empty"**
- Manually empty bucket: `aws s3 rm s3://bucket-name --recursive`
- Then run cleanup again

**Error: "Security group in use"**
- Wait for instances to fully terminate (may take a few minutes)
- Then run cleanup again

## Advanced Usage

### Spot Instances

To use spot instances (cheaper, but can be interrupted):

Modify `setup_stack()` in script to add:
```bash
--instance-market-options 'MarketType=spot,SpotOptions={MaxPrice=0.10}'
```

### Multiple Regions

Test in different regions:
```bash
export AWS_REGION=us-west-2
./scripts/aws_benchmark.sh setup
```

### Custom Instance Types

Test with different instance sizes:
```bash
export INSTANCE_TYPE=c5.4xlarge  # 16 vCPU, 32GB RAM
./scripts/aws_benchmark.sh setup
```

### Persistent State

To keep instances running between benchmarks:
1. Don't run `cleanup` after setup
2. Run multiple `run` commands
3. Run `cleanup` when done

**Warning:** Instances will continue to incur charges until terminated!

## Security Notes

- **SSH Keys**: Private key is stored in `~/.ssh/`. Keep it secure.
- **Security Groups**: Script opens port 22 to 0.0.0.0/0 for simplicity. Restrict in production.
- **IAM Roles**: Uses IAM roles (no hardcoded keys). Best practice for AWS.
- **S3 Bucket**: Bucket is created with default settings. Add encryption/bucket policies as needed.

## Next Steps

After benchmarking:
1. **Analyze results**: Compare against design goals
2. **Optimize bottlenecks**: Identify slow query types
3. **Tune configuration**: Adjust based on workload
4. **Scale testing**: Test with higher QPS/concurrency
5. **Cost analysis**: Review AWS costs in console

## Support

For issues:
1. Check CloudWatch logs
2. SSH to instances and check application logs
3. Review `aws_benchmark_state.json` for resource IDs
4. Check AWS console for resource status
