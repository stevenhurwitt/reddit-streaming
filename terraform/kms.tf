data "template_file" "kms_policy_template" {
    template = file("${path.module}/policies/kms_policy.json")
    vars = {
        aws_account_id = local.aws_account_id
    }
}

# ecr kms key
resource "aws_kms_key" "gps_ecr_cmk" {
    description = "KMS CMK for ECR image repo in ${local.env} environment"
    policy = data.template_file.kms_policy_template.rendered 
    enable_key_rotation = true 
    tags = merge(local.product_tags, map("description", "KMS CMK for ECR image repo"))
}

resource "aws_kms_alias" "gps_ecr_image_cmk_alias" {
    name                = "alias/${local.product}-ecr-cmk-alias${local.environment}"
    target_key_id       = aws_kms_key.gps_ecr_cmk.key_id
    depends_on          = [aws_kms_key.gps_ecr_cmk]
}

# secrets manager kms keys
resource "aws_kms_key" "gps_sm_db_master_cmk" {
    description = "KMS CMK for Secrets Manager DB master secret ${local.env} environment"
    policy = data.template_file.kms_policy_template.rendered 
    enable_key_rotation = true 
    tags = merge(local.product_tags, map("description", "KMS CMK for Secrets Manager DB master secret"))
}

resource "aws_kms_alias" "gps_sm_db_master_cmk_alias" {
    name                = "alias/${local.product}-sm-db-master-cmk-alias${local.environment}"
    target_key_id       = aws_kms_key.gps_sm_db_master_cmk.key_id 
    depends_on          = [aws_kms_key.gps_sm_db_master_cmk]
}

# azure secret
resource "aws_kms_key" "gps_sm_azure_cmk" {
    description = "KMS CMK for Secrets Manager Azure secret ${local.env} environment"
    policy = data.template_file.kms_policy_template.rendered 
    enable_key_rotation = true 
    tags = merge(local.product_tags, map("description", "KMS CMK for Secrets Manager Azure secret"))
}

resource "aws_kms_alias" "gps_sm_azure_cmk_alias" {
    name                = "alias/${local.product}-sm-azure-cmk-alias${local.environment}"
    target_key_id       = aws_kms_key.gps_sm_azure_cmk.key_id 
    depends_on          = [aws_kms_key.gps_sm_azure_cmk]
}

# s3 key
resource "aws_kms_key" "gps_bucket_kms_key" {
    description = "This key is used to encrypt bucket objects"
    policy = data.template_file.kms_policy_template.rendered 
    deletion_window_in_days = 10
    enable_key_rotation = true 
    tags = merge(local.product_tags, map("description", "KMS CMK for GPS S3 bucket"))
}