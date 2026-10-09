terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

variable "endpoint" {
  default = "http://localhost:4566"
}

# Floci accepts any credentials. Every service is pointed at the one local endpoint.
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true # otherwise S3 Control calls go to <account>.localhost

  endpoints {
    s3             = var.endpoint
    s3control      = var.endpoint
    glue           = var.endpoint
    sqs            = var.endpoint
    athena         = var.endpoint
    rds            = var.endpoint
    kafka          = var.endpoint
    lambda         = var.endpoint
    sfn            = var.endpoint
    events         = var.endpoint
    scheduler      = var.endpoint
    secretsmanager = var.endpoint
    cloudwatch     = var.endpoint
    logs           = var.endpoint
    iam            = var.endpoint
    sts            = var.endpoint
  }
}

resource "aws_s3_bucket" "lake" {
  bucket = "lossproof-lake"
}

resource "aws_glue_catalog_database" "lake" {
  name       = "lossproof"
  catalog_id = "000000000000" # explicit because the account id is not requested
}

resource "aws_sqs_queue" "dlq" {
  name = "lossproof-dlq"
}

# --- Source database -------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "source" {
  name = "lossproof/source-db"
}

resource "aws_secretsmanager_secret_version" "source" {
  secret_id     = aws_secretsmanager_secret.source.id
  secret_string = jsonencode({ username = "lp", password = "lossproof1", dbname = "shop" }) # local emulator only
}

# On AWS this parameter turns on logical decoding. Floci accepts it but does not apply it, so
# scripts/up.sh also sets wal_level on the container (docs/adr/001-floci-spike.md).
resource "aws_db_parameter_group" "logical" {
  name   = "lossproof-logical"
  family = "postgres16"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  # Floci does not return parameters, so without this every plan shows a change.
  lifecycle {
    ignore_changes = [parameter]
  }
}

resource "aws_db_instance" "source" {
  identifier           = "lossproof-source"
  engine               = "postgres"
  engine_version       = "16"
  instance_class       = "db.t3.micro"
  allocated_storage    = 5
  db_name              = "shop"
  username             = "lp"
  password             = "lossproof1"
  parameter_group_name = aws_db_parameter_group.logical.name
  skip_final_snapshot  = true
}

# --- Change stream ---------------------------------------------------------------------------

resource "aws_msk_cluster" "stream" {
  cluster_name           = "lossproof-stream"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = ["subnet-00000000"]
    security_groups = ["sg-00000000"]
  }

  # Clients on the Compose network connect without TLS. This is also what Floci reports back.
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
    }
  }
}

output "kafka_bootstrap" {
  value = aws_msk_cluster.stream.bootstrap_brokers
}

output "source_resource_id" {
  value = aws_db_instance.source.resource_id
}
