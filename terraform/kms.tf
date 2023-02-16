data "template_file" "kms_policy_template" {
    template = file("${path.module}/policies/kms_policy.json")
    vars = {
        aws_account_id = var.aws_account_id
    }
}

# RDS KMS Key
resource "aws_kms_key" "rds_cmk" {
    description = "KMS CMK for RDS Aurora Postgresql in ${var.environment} environment."
    policy = data.template_file.kms_policy_template.rendered
    enable_key_rotation = true
    tags = {
        "description" = "KMS CMK for RDS Aurora Postgresql"
    }
}

resource "aws_kms_alias" "rds_cmk_alias" {
    name = "alias/${var.product}-rds-cmk-alias${var.env_suffix}"
    target_key_id = aws_kms_key.rds_cmk.key_id
    depends_on = [
        aws_kms_key.rds_cmk
    ]
}