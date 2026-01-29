#!/bin/bash
# AWS Benchmark Setup Script
#
# Manages complete AWS benchmark stack: EC2 instances, S3 bucket, REST catalog, and application
#
# Usage:
#   ./scripts/aws_benchmark.sh setup
#   ./scripts/aws_benchmark.sh run [--duration 300] [--span-qps 100] [--log-qps 100] [--metric-qps 100] [--query-concurrency 4] [--query-interval-ms 500]
#   ./scripts/aws_benchmark.sh cleanup
#
# Prerequisites:
#   - AWS CLI installed and configured
#   - AWS credentials with EC2, S3, IAM permissions
#   - SSH key pair in AWS (or script will create one)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$PROJECT_ROOT/aws_benchmark_state.json"

# Configuration
INSTANCE_TYPE="${INSTANCE_TYPE:-c5.2xlarge}"
REGION="${AWS_REGION:-us-east-1}"
AMI_ID="${AMI_ID:-}"  # Will auto-detect Ubuntu 22.04 if not set
KEY_NAME="${KEY_NAME:-datalake-benchmark-key}"
SECURITY_GROUP_NAME="${SECURITY_GROUP_NAME:-datalake-benchmark-sg}"
IAM_ROLE_NAME="${IAM_ROLE_NAME:-datalake-benchmark-role}"
IAM_POLICY_NAME="${IAM_POLICY_NAME:-datalake-benchmark-policy}"
BUCKET_PREFIX="${BUCKET_PREFIX:-datalake-benchmark}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Install it from https://aws.amazon.com/cli/"
        exit 1
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Run 'aws configure'"
        exit 1
    fi
    
    local account_id=$(aws sts get-caller-identity --query Account --output text)
    log_info "Using AWS account: $account_id"
    
    # Check permissions
    if ! aws ec2 describe-instances --max-items 1 &> /dev/null; then
        log_error "Missing EC2 permissions"
        exit 1
    fi
    
    if ! aws s3 ls &> /dev/null; then
        log_error "Missing S3 permissions"
        exit 1
    fi
    
    log_info "Prerequisites check passed"
}

# Get default VPC ID
get_default_vpc() {
    local vpc_id=$(aws ec2 describe-vpcs \
        --filters "Name=isDefault,Values=true" \
        --query "Vpcs[0].VpcId" \
        --output text \
        --region "$REGION" 2>/dev/null || echo "")
    
    if [ -z "$vpc_id" ] || [ "$vpc_id" == "None" ]; then
        log_error "No default VPC found in region $REGION"
        exit 1
    fi
    
    echo "$vpc_id"
}

# Get Ubuntu 22.04 AMI ID
get_ubuntu_ami() {
    local ami_id=$(aws ec2 describe-images \
        --owners 099720109477 \
        --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
                  "Name=state,Values=available" \
        --query "Images | sort_by(@, &CreationDate) | [-1].ImageId" \
        --output text \
        --region "$REGION")
    
    if [ -z "$ami_id" ] || [ "$ami_id" == "None" ]; then
        log_error "Failed to find Ubuntu 22.04 AMI in region $REGION"
        exit 1
    fi
    
    echo "$ami_id"
}

# Create or get SSH key pair
ensure_ssh_key() {
    log_info "Checking SSH key pair: $KEY_NAME"
    
    if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &> /dev/null; then
        log_info "SSH key pair '$KEY_NAME' already exists"
        return
    fi
    
    log_info "Creating SSH key pair: $KEY_NAME"
    local key_file="$HOME/.ssh/${KEY_NAME}.pem"
    
    if [ -f "$key_file" ]; then
        log_warn "Key file $key_file already exists. Importing to AWS..."
        aws ec2 import-key-pair \
            --key-name "$KEY_NAME" \
            --public-key-material "fileb://${key_file}.pub" \
            --region "$REGION"
    else
        log_info "Generating new SSH key pair..."
        ssh-keygen -t rsa -b 4096 -f "$key_file" -N "" -C "datalake-benchmark"
        
        aws ec2 import-key-pair \
            --key-name "$KEY_NAME" \
            --public-key-material "fileb://${key_file}.pub" \
            --region "$REGION"
        
        chmod 600 "$key_file"
        log_info "SSH key saved to: $key_file"
        log_warn "Save this key file - you'll need it to SSH to instances"
    fi
}

# Create security group
create_security_group() {
    log_info "Creating security group: $SECURITY_GROUP_NAME"
    
    local vpc_id=$(get_default_vpc)
    
    # Check if exists
    local sg_id=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$vpc_id" \
        --query "SecurityGroups[0].GroupId" \
        --output text \
        --region "$REGION" 2>/dev/null || echo "")
    
    if [ -n "$sg_id" ] && [ "$sg_id" != "None" ]; then
        log_info "Security group already exists: $sg_id"
        echo "$sg_id"
        return
    fi
    
    # Create security group
    sg_id=$(aws ec2 create-security-group \
        --group-name "$SECURITY_GROUP_NAME" \
        --description "Security group for datalake benchmark" \
        --vpc-id "$vpc_id" \
        --region "$REGION" \
        --query "GroupId" \
        --output text)
    
    # Allow SSH from anywhere (for simplicity - restrict in production)
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" \
        --protocol tcp \
        --port 22 \
        --cidr 0.0.0.0/0 \
        --region "$REGION" &> /dev/null || true
    
    # Allow catalog port from within VPC
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" \
        --protocol tcp \
        --port 8181 \
        --source-group "$sg_id" \
        --region "$REGION" &> /dev/null || true
    
    # Allow all outbound
    aws ec2 authorize-security-group-egress \
        --group-id "$sg_id" \
        --protocol -1 \
        --cidr 0.0.0.0/0 \
        --region "$REGION" &> /dev/null || true
    
    log_info "Security group created: $sg_id"
    echo "$sg_id"
}

# Create IAM role for EC2 instances
create_iam_role() {
    log_info "Creating IAM role: $IAM_ROLE_NAME"
    
    # Check if role exists
    if aws iam get-role --role-name "$IAM_ROLE_NAME" &> /dev/null; then
        log_info "IAM role already exists"
        return
    fi
    
    # Create trust policy
    cat > /tmp/trust-policy.json <<EOF
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
EOF
    
    # Create role
    aws iam create-role \
        --role-name "$IAM_ROLE_NAME" \
        --assume-role-policy-document file:///tmp/trust-policy.json
    
    # Create policy for S3 access
    cat > /tmp/s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::*",
        "arn:aws:s3:::*/*"
      ]
    }
  ]
}
EOF
    
    # Create policy
    aws iam put-role-policy \
        --role-name "$IAM_ROLE_NAME" \
        --policy-name "$IAM_POLICY_NAME" \
        --policy-document file:///tmp/s3-policy.json
    
    # Create instance profile
    if ! aws iam get-instance-profile --instance-profile-name "$IAM_ROLE_NAME" &> /dev/null; then
        aws iam create-instance-profile --instance-profile-name "$IAM_ROLE_NAME"
        aws iam add-role-to-instance-profile \
            --instance-profile-name "$IAM_ROLE_NAME" \
            --role-name "$IAM_ROLE_NAME"
        
        # Wait for propagation
        sleep 5
    fi
    
    log_info "IAM role created"
    rm -f /tmp/trust-policy.json /tmp/s3-policy.json
}

# Create S3 bucket
create_s3_bucket() {
    local bucket_name="${BUCKET_PREFIX}-$(date +%s)"
    log_info "Creating S3 bucket: $bucket_name"
    
    # Create bucket
    if [ "$REGION" == "us-east-1" ]; then
        aws s3api create-bucket --bucket "$bucket_name" --region "$REGION"
    else
        aws s3api create-bucket \
            --bucket "$bucket_name" \
            --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION"
    fi
    
    # Wait for bucket to be ready
    sleep 2
    
    log_info "S3 bucket created: $bucket_name"
    echo "$bucket_name"
}

# Wait for instance to be running and get IP
wait_for_instance() {
    local instance_id=$1
    local max_wait=300
    local elapsed=0
    
    log_info "Waiting for instance $instance_id to be running..."
    
    while [ $elapsed -lt $max_wait ]; do
        local state=$(aws ec2 describe-instances \
            --instance-ids "$instance_id" \
            --query "Reservations[0].Instances[0].State.Name" \
            --output text \
            --region "$REGION")
        
        if [ "$state" == "running" ]; then
            break
        fi
        
        sleep 5
        elapsed=$((elapsed + 5))
        echo -n "."
    done
    echo
    
    if [ "$state" != "running" ]; then
        log_error "Instance failed to start"
        exit 1
    fi
    
    # Get public IP (remove any newlines/whitespace)
    local public_ip=$(aws ec2 describe-instances \
        --instance-ids "$instance_id" \
        --query "Reservations[0].Instances[0].PublicIpAddress" \
        --output text \
        --region "$REGION" | tr -d '\n\r\t ')
    
    # Wait for SSH to be ready
    log_info "Waiting for SSH to be ready on $public_ip..."
    local ssh_ready=false
    for i in {1..30}; do
        if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
           -i "$HOME/.ssh/${KEY_NAME}.pem" \
           ubuntu@"$public_ip" "echo 'SSH ready'" &> /dev/null; then
            ssh_ready=true
            break
        fi
        sleep 5
    done
    
    if [ "$ssh_ready" != "true" ]; then
        log_error "SSH not ready after 150 seconds"
        exit 1
    fi
    
    # Return only the IP, stripping any leading dots or whitespace
    echo "$public_ip" | sed 's/^[. ]*//' | tr -d '\n\r\t '
}

# Deploy REST catalog (Lakekeeper) on instance
deploy_rest_catalog() {
    local instance_id=$1
    local public_ip=$2
    local bucket_name=$3
    local key_file="$HOME/.ssh/${KEY_NAME}.pem"
    
    log_info "Deploying REST catalog on $public_ip..."
    
    # Create deployment script
    cat > /tmp/deploy_catalog.sh <<'DEPLOY_EOF'
#!/bin/bash
set -e

# Update system
sudo apt-get update -y
sudo apt-get install -y docker.io docker-compose curl python3

# Start Docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ubuntu

# Create docker-compose.yml for Lakekeeper
mkdir -p ~/lakekeeper
cat > ~/lakekeeper/docker-compose.yml <<'EOF'
version: '3.8'
services:
  db:
    image: postgres:17
    environment:
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_USER=postgres
      - POSTGRES_DB=postgres
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 10

  migrate:
    image: vakamo/lakekeeper:latest
    environment:
      - LAKEKEEPER__PG_ENCRYPTION_KEY=benchmark-key-change-in-production
      - LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
      - LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
    command: ["migrate"]
    depends_on:
      db:
        condition: service_healthy

  lakekeeper:
    image: vakamo/lakekeeper:latest
    ports:
      - "8181:8181"
    environment:
      - LAKEKEEPER__PG_ENCRYPTION_KEY=benchmark-key-change-in-production
      - LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
      - LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
    command: ["serve"]
    depends_on:
      db:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "/home/nonroot/lakekeeper", "healthcheck"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  postgres-data:
EOF

cd ~/lakekeeper
sudo docker-compose up -d

# Wait for services
echo "Waiting for Lakekeeper to be ready..."
for i in {1..60}; do
    if curl -sf http://localhost:8181/health &> /dev/null; then
        echo "Lakekeeper is ready"
        break
    fi
    sleep 2
done

# Bootstrap Lakekeeper
curl -s -X POST http://localhost:8181/management/v1/bootstrap \
    -H "Content-Type: application/json" \
    -d '{"accept-terms-of-use": true}' || true

# Get AWS credentials from instance metadata
AWS_ACCESS_KEY_ID=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/datalake-benchmark-role | python3 -c "import sys, json; print(json.load(sys.stdin)['AccessKeyId'])" 2>/dev/null || echo "")
AWS_SECRET_ACCESS_KEY=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/datalake-benchmark-role | python3 -c "import sys, json; print(json.load(sys.stdin)['SecretAccessKey'])" 2>/dev/null || echo "")
AWS_SESSION_TOKEN=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/datalake-benchmark-role | python3 -c "import sys, json; print(json.load(sys.stdin)['Token'])" 2>/dev/null || echo "")

# Create warehouse
WAREHOUSE_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:8181/management/v1/warehouse \
    -H "Content-Type: application/json" \
    -d "{
        \"warehouse-name\": \"default\",
        \"project-id\": \"00000000-0000-0000-0000-000000000000\",
        \"storage-profile\": {
            \"type\": \"s3\",
            \"bucket\": \"BUCKET_NAME_PLACEHOLDER\",
            \"key-prefix\": \"iceberg\",
            \"region\": \"REGION_PLACEHOLDER\"
        },
        \"storage-credential\": {
            \"type\": \"s3\",
            \"credential-type\": \"iam-role\"
        }
    }")

HTTP_CODE=$(echo "$WAREHOUSE_RESPONSE" | tail -n1)
if [ "$HTTP_CODE" != "201" ] && [ "$HTTP_CODE" != "400" ]; then
    echo "Warehouse creation failed: $HTTP_CODE"
    echo "$WAREHOUSE_RESPONSE"
    exit 1
fi

echo "REST catalog deployed successfully"
DEPLOY_EOF
    
    # Replace placeholders using Python to avoid sed escaping issues
    echo "$bucket_name" > /tmp/bucket_name.txt
    echo "$REGION" > /tmp/region.txt
    python3 << 'PYEOF'
with open('/tmp/bucket_name.txt', 'r') as f:
    bucket_name = f.read().strip()
with open('/tmp/region.txt', 'r') as f:
    region = f.read().strip()
with open('/tmp/deploy_catalog.sh', 'r') as f:
    content = f.read()
content = content.replace('BUCKET_NAME_PLACEHOLDER', bucket_name)
content = content.replace('REGION_PLACEHOLDER', region)
with open('/tmp/deploy_catalog.sh', 'w') as f:
    f.write(content)
PYEOF
    rm -f /tmp/bucket_name.txt /tmp/region.txt
    
    # Copy and execute
    scp -i "$key_file" -o StrictHostKeyChecking=no \
        /tmp/deploy_catalog.sh ubuntu@"$public_ip":/tmp/deploy_catalog.sh
    
    ssh -i "$key_file" -o StrictHostKeyChecking=no ubuntu@"$public_ip" \
        "chmod +x /tmp/deploy_catalog.sh && sudo /tmp/deploy_catalog.sh"
    
    # Wait for catalog to be ready
    log_info "Waiting for REST catalog to be ready..."
    for i in {1..60}; do
        if ssh -i "$key_file" -o StrictHostKeyChecking=no ubuntu@"$public_ip" \
           "curl -sf http://localhost:8181/health" &> /dev/null; then
            log_info "REST catalog is ready"
            break
        fi
        sleep 2
    done
    
    rm -f /tmp/deploy_catalog.sh
}

# Deploy application on instance
deploy_application() {
    local instance_id=$1
    local public_ip=$2
    local catalog_ip=$3
    local bucket_name=$4
    local key_file="$HOME/.ssh/${KEY_NAME}.pem"
    
    log_info "Deploying application on $public_ip..."
    
    # Create deployment script
    cat > /tmp/deploy_app.sh <<'DEPLOY_EOF'
#!/bin/bash
set -e

# Update system
sudo apt-get update -y
sudo apt-get install -y build-essential pkg-config libssl-dev curl

# Install Rust
if ! command -v rustc &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source ~/.cargo/env
fi

# Clone or copy application
if [ -d ~/datalake ]; then
    cd ~/datalake
    git pull || true
else
    # Copy code via SCP (will be done after script creation)
    mkdir -p ~/datalake
fi

cd ~/datalake

# Create config file
cat > config-aws-benchmark.yaml <<EOF
server:
  port: 8090
  host: "0.0.0.0"
  max_body_size: 104857600
  worker_threads: null

storage:
  s3_region: "$REGION"

span_buffering:
  max_buffer_bytes: 134217728
  max_buffer_spans: 10000
  flush_interval_seconds: 60

ingest_engine:
  enabled: true
  wal_bucket: "BUCKET_NAME_PLACEHOLDER"
  wal_prefix: "wal"
  cache_dir: "/var/tmp/datalake/cache"
  optimizer_interval_seconds: 300
  replay_wal_on_startup: false

compaction:
  enabled: true
  min_files_to_compact: 5
  target_file_size_bytes: 67108864
  compaction_interval_seconds: 3600

duckdb:
  max_connections: 4
  max_memory_per_query: "1GB"
  max_query_duration_seconds: 60
  enable_spill_to_disk: true
  spill_directory: "/var/tmp/datalake/duckdb_spill"

s3:
  endpoint: null
  access_key_id: null
  secret_access_key: null

iceberg:
  catalog_type: "rest"
  catalog_uri: "http://CATALOG_IP_PLACEHOLDER:8181/catalog"
  catalog_token: null
  warehouse: "default"
  write_target_file_size_bytes: 134217728
  write_row_group_size_bytes: 134217728
  write_page_size_bytes: 1048576
  force_close_after_append: false
EOF

# Create cache directories
sudo mkdir -p /var/tmp/datalake/cache
sudo mkdir -p /var/tmp/datalake/duckdb_spill
sudo chown -R ubuntu:ubuntu /var/tmp/datalake

# Add swap space to prevent OOM (8GB swap for c5.2xlarge with 16GB RAM)
if [ ! -f /swapfile ]; then
    echo "Creating 8GB swap file..."
    sudo dd if=/dev/zero of=/swapfile bs=1M count=8192
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "Swap created: $(free -h | grep Swap)"
fi

# Set up automatic cache cleanup to prevent disk full
sudo tee /usr/local/bin/cleanup-datalake-cache > /dev/null << 'CLEANUP_EOF'
#!/bin/bash
# Clean up old parquet files older than 1 hour to prevent disk full
find /var/tmp/datalake/cache -name "*.parquet" -type f -mmin +60 -delete 2>/dev/null
find /var/tmp/datalake/cache -type d -empty -delete 2>/dev/null
CLEANUP_EOF
sudo chmod +x /usr/local/bin/cleanup-datalake-cache
echo "*/10 * * * * root /usr/local/bin/cleanup-datalake-cache" | sudo tee -a /etc/crontab

# Build application
cargo build --release --bin perf_stress

echo "Application deployed successfully"
DEPLOY_EOF
    
    # Replace placeholders using Python
    echo "$REGION" > /tmp/region_deploy.txt
    echo "$catalog_ip" > /tmp/catalog_ip_deploy.txt
    echo "$bucket_name" > /tmp/bucket_name_deploy.txt
    python3 << 'PYEOF'
with open('/tmp/region_deploy.txt', 'r') as f:
    region = f.read().strip()
with open('/tmp/catalog_ip_deploy.txt', 'r') as f:
    catalog_ip = f.read().strip()
with open('/tmp/bucket_name_deploy.txt', 'r') as f:
    bucket_name = f.read().strip()
with open('/tmp/deploy_app.sh', 'r') as f:
    content = f.read()
content = content.replace('REGION_PLACEHOLDER', region)
content = content.replace('CATALOG_IP_PLACEHOLDER', catalog_ip)
content = content.replace('BUCKET_NAME_PLACEHOLDER', bucket_name)
with open('/tmp/deploy_app.sh', 'w') as f:
    f.write(content)
PYEOF
    rm -f /tmp/region_deploy.txt /tmp/catalog_ip_deploy.txt /tmp/bucket_name_deploy.txt
    
    # Copy application code and deployment script
    log_info "Copying application code to instance..."
    scp -i "$key_file" -o StrictHostKeyChecking=no -r \
        "$PROJECT_ROOT" ubuntu@"$public_ip":~/datalake 2>&1 | grep -v "Warning: Permanently added" || true
    
    # Copy and execute deployment script
    scp -i "$key_file" -o StrictHostKeyChecking=no \
        /tmp/deploy_app.sh ubuntu@"$public_ip":/tmp/deploy_app.sh
    
    ssh -i "$key_file" -o StrictHostKeyChecking=no ubuntu@"$public_ip" \
        "chmod +x /tmp/deploy_app.sh && /tmp/deploy_app.sh"
    
    rm -f /tmp/deploy_app.sh
}

# Setup stack
setup_stack() {
    log_info "Setting up AWS benchmark stack..."
    
    check_prerequisites
    ensure_ssh_key
    create_iam_role
    
    local sg_id=$(create_security_group)
    local bucket_name=$(create_s3_bucket)
    
    # Get AMI
    if [ -z "$AMI_ID" ]; then
        AMI_ID=$(get_ubuntu_ami)
    fi
    log_info "Using AMI: $AMI_ID"
    
    # Get subnet in default VPC
    local vpc_id=$(get_default_vpc)
    local subnet_id=$(aws ec2 describe-subnets \
        --filters "Name=vpc-id,Values=$vpc_id" \
        --query "Subnets[0].SubnetId" \
        --output text \
        --region "$REGION")
    
    # Launch catalog instance
    log_info "Launching REST catalog instance..."
    local catalog_instance_id=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$sg_id" \
        --subnet-id "$subnet_id" \
        --iam-instance-profile Name="$IAM_ROLE_NAME" \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=datalake-benchmark-catalog}]" \
        --query "Instances[0].InstanceId" \
        --output text \
        --region "$REGION")
    
    local catalog_ip=$(wait_for_instance "$catalog_instance_id" 2>&1 | tail -1 | sed 's/^[. ]*//' | tr -d '\n\r\t ')
    log_info "Catalog instance ready: $catalog_ip"
    
    # Deploy catalog
    deploy_rest_catalog "$catalog_instance_id" "$catalog_ip" "$bucket_name"
    
    # Launch app instance
    log_info "Launching application instance..."
    local app_instance_id=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$sg_id" \
        --subnet-id "$subnet_id" \
        --iam-instance-profile Name="$IAM_ROLE_NAME" \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=datalake-benchmark-app}]" \
        --query "Instances[0].InstanceId" \
        --output text \
        --region "$REGION")
    
    local app_ip=$(wait_for_instance "$app_instance_id")
    app_ip=$(echo "$app_ip" | tr -d '\n\r\t ')
    log_info "Application instance ready: $app_ip"
    
    # Deploy application
    deploy_application "$app_instance_id" "$app_ip" "$catalog_ip" "$bucket_name"
    
    # Save state
    cat > "$STATE_FILE" <<EOF
{
  "catalog_instance_id": "$catalog_instance_id",
  "catalog_ip": "$catalog_ip",
  "app_instance_id": "$app_instance_id",
  "app_ip": "$app_ip",
  "s3_bucket": "$bucket_name",
  "region": "$REGION",
  "security_group_id": "$sg_id",
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    
    log_info "Setup complete!"
    log_info "Catalog endpoint: http://$catalog_ip:8181/catalog"
    log_info "Application IP: $app_ip"
    log_info "S3 bucket: $bucket_name"
    log_info "State saved to: $STATE_FILE"
}

# Run benchmark
run_benchmark() {
    if [ ! -f "$STATE_FILE" ]; then
        log_error "State file not found. Run 'setup' first."
        exit 1
    fi
    
    # Check if jq is available, otherwise use python
    if command -v jq &> /dev/null; then
        local catalog_instance_id=$(jq -r '.catalog_instance_id' "$STATE_FILE")
        local catalog_ip=$(jq -r '.catalog_ip' "$STATE_FILE")
        local app_instance_id=$(jq -r '.app_instance_id' "$STATE_FILE")
        local app_ip=$(jq -r '.app_ip' "$STATE_FILE")
        local bucket_name=$(jq -r '.s3_bucket' "$STATE_FILE")
    else
        local catalog_instance_id=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['catalog_instance_id'])" 2>/dev/null)
        local catalog_ip=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['catalog_ip'])" 2>/dev/null)
        local app_instance_id=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['app_instance_id'])" 2>/dev/null)
        local app_ip=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['app_ip'])" 2>/dev/null)
        local bucket_name=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['s3_bucket'])" 2>/dev/null)
    fi
    local key_file="$HOME/.ssh/${KEY_NAME}.pem"
    
    # Verify setup
    log_info "Verifying setup..."
    
    # Check instances are running
    local catalog_state=$(aws ec2 describe-instances \
        --instance-ids "$catalog_instance_id" \
        --query "Reservations[0].Instances[0].State.Name" \
        --output text \
        --region "$REGION" 2>/dev/null || echo "unknown")
    
    if [ "$catalog_state" != "running" ]; then
        log_error "Catalog instance is not running (state: $catalog_state)"
        exit 1
    fi
    
    local app_state=$(aws ec2 describe-instances \
        --instance-ids "$app_instance_id" \
        --query "Reservations[0].Instances[0].State.Name" \
        --output text \
        --region "$REGION" 2>/dev/null || echo "unknown")
    
    if [ "$app_state" != "running" ]; then
        log_error "Application instance is not running (state: $app_state)"
        exit 1
    fi
    
    # Verify REST catalog is accessible
    log_info "Checking REST catalog accessibility..."
    if ! ssh -i "$key_file" -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
        ubuntu@"$catalog_ip" "curl -sf http://localhost:8181/health" &> /dev/null; then
        log_error "REST catalog is not accessible on $catalog_ip:8181"
        exit 1
    fi
    
    # Verify S3 bucket exists
    log_info "Checking S3 bucket..."
    if ! aws s3api head-bucket --bucket "$bucket_name" --region "$REGION" &> /dev/null; then
        log_error "S3 bucket does not exist: $bucket_name"
        exit 1
    fi
    
    log_info "Setup verification passed"
    
    # Parse benchmark arguments
    local duration=300
    local span_qps=100
    local log_qps=100
    local metric_qps=100
    local query_concurrency=4
    local query_interval_ms=500
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --duration)
                duration="$2"
                shift 2
                ;;
            --span-qps)
                span_qps="$2"
                shift 2
                ;;
            --log-qps)
                log_qps="$2"
                shift 2
                ;;
            --metric-qps)
                metric_qps="$2"
                shift 2
                ;;
            --query-concurrency)
                query_concurrency="$2"
                shift 2
                ;;
            --query-interval-ms)
                query_interval_ms="$2"
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done
    
    log_info "Running benchmark on $app_ip..."
    log_info "Parameters: duration=${duration}s, span_qps=${span_qps}, log_qps=${log_qps}, metric_qps=${metric_qps}"
    
    # Run benchmark
    ssh -i "$key_file" -o StrictHostKeyChecking=no ubuntu@"$app_ip" \
        "cd ~/datalake && \
         RUST_LOG=info \
         CONFIG_FILE=config-aws-benchmark.yaml \
         cargo run --release --bin perf_stress -- \
         --duration $duration \
         --warmup-secs 60 \
         --span-qps $span_qps \
         --log-qps $log_qps \
         --metric-qps $metric_qps \
         --query-concurrency $query_concurrency \
         --query-interval-ms $query_interval_ms"
    
    log_info "Benchmark complete!"
}

# Cleanup stack
cleanup_stack() {
    if [ ! -f "$STATE_FILE" ]; then
        log_warn "State file not found. Nothing to clean up."
        exit 0
    fi
    
    log_info "Cleaning up AWS resources..."
    
    # Check if jq is available, otherwise use python
    if command -v jq &> /dev/null; then
        local catalog_instance_id=$(jq -r '.catalog_instance_id' "$STATE_FILE")
        local app_instance_id=$(jq -r '.app_instance_id' "$STATE_FILE")
        local bucket_name=$(jq -r '.s3_bucket' "$STATE_FILE")
        local sg_id=$(jq -r '.security_group_id' "$STATE_FILE")
    else
        local catalog_instance_id=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['catalog_instance_id'])" 2>/dev/null)
        local app_instance_id=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['app_instance_id'])" 2>/dev/null)
        local bucket_name=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['s3_bucket'])" 2>/dev/null)
        local sg_id=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['security_group_id'])" 2>/dev/null)
    fi
    
    # Terminate instances
    if [ "$catalog_instance_id" != "null" ] && [ -n "$catalog_instance_id" ]; then
        log_info "Terminating catalog instance: $catalog_instance_id"
        aws ec2 terminate-instances \
            --instance-ids "$catalog_instance_id" \
            --region "$REGION" &> /dev/null || true
    fi
    
    if [ "$app_instance_id" != "null" ] && [ -n "$app_instance_id" ]; then
        log_info "Terminating application instance: $app_instance_id"
        aws ec2 terminate-instances \
            --instance-ids "$app_instance_id" \
            --region "$REGION" &> /dev/null || true
    fi
    
    # Empty and delete S3 bucket
    if [ "$bucket_name" != "null" ] && [ -n "$bucket_name" ]; then
        log_info "Deleting S3 bucket: $bucket_name"
        aws s3 rm "s3://$bucket_name" --recursive &> /dev/null || true
        aws s3api delete-bucket --bucket "$bucket_name" --region "$REGION" &> /dev/null || true
    fi
    
    # Delete security group
    if [ "$sg_id" != "null" ] && [ -n "$sg_id" ]; then
        log_info "Deleting security group: $sg_id"
        aws ec2 delete-security-group --group-id "$sg_id" --region "$REGION" &> /dev/null || true
    fi
    
    # Delete IAM resources
    log_info "Cleaning up IAM resources..."
    aws iam remove-role-from-instance-profile \
        --instance-profile-name "$IAM_ROLE_NAME" \
        --role-name "$IAM_ROLE_NAME" &> /dev/null || true
    
    aws iam delete-instance-profile \
        --instance-profile-name "$IAM_ROLE_NAME" &> /dev/null || true
    
    aws iam delete-role-policy \
        --role-name "$IAM_ROLE_NAME" \
        --policy-name "$IAM_POLICY_NAME" &> /dev/null || true
    
    aws iam delete-role \
        --role-name "$IAM_ROLE_NAME" &> /dev/null || true
    
    # Remove state file
    rm -f "$STATE_FILE"
    
    log_info "Cleanup complete!"
}

# Main
case "${1:-}" in
    setup)
        setup_stack
        ;;
    run)
        shift
        run_benchmark "$@"
        ;;
    cleanup)
        cleanup_stack
        ;;
    *)
        echo "Usage: $0 {setup|run|cleanup}"
        echo ""
        echo "Commands:"
        echo "  setup   - Provision EC2 instances, S3 bucket, and deploy services"
        echo "  run     - Execute benchmark (supports --duration, --span-qps, etc.)"
        echo "  cleanup - Terminate instances and delete all resources"
        exit 1
        ;;
esac
