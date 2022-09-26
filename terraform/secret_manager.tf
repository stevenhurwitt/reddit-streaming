resource "aws_secretsmanager_secret" "gps_app_db_master_secret" {
    name                    = "${local.product}-app-db-master-secret${local.environment}"
    kms_key_id              = aws_kms_alias.gps_sm_db_master_cmk_alias.target_key_arn
    recovery_window_in_days = 0
    tags                    = merge(local.product_tags, map("name", "app-db-master-secret"))
}

resource "aws_secretsmanager_secret" "gps_app_azure_secret" {
    name                    = "${local.product}-app-azure-secret${local.environment}"
    kms_key_id              = aws_kms_alias.gps_sm_azure_cmk_alias.target_key_arn
    recovery_window_in_days = 0
    tags                    = merge(local.product_tags, map("name", "app-azure-secret"))
}

resource "aws_secretsmanager_secret_version" "gps_app_db_master_secret_version" {
    secret_id           = aws_secretsmanager_secret.gps_app_db_master_secret 
    secret_string = jsonencode({
        username = local.db_master_username,
        password = local.db_master_password,
        host = aws_rds_cluster.gps_mysql_cluster.endpoint,
        engine = "mysql",
        port = aws_rds_cluster.gps_mysql_cluster.port,
        dname = aws_rds_cluster.gps_mysql_cluster.database_name
    })

    depends_on = [
        aws_secretsmanager_secret.gps_app_db_master_secret, aws_rds_cluster.gps_mysql_cluster
    ]

    lifecycle {
        ignore_changes = [
            secret_string
        ]
    }
}

resource "aws_secretsmanager_secret_version" "gps_app_azure_secret_version" {
    secret_id = aws_secretsmanager_secret.gps_app_azure_secret.id
    secret_string = jsonencode({
        client-id = "",
        client-secret-id = "",
        tenant-id = ""
    })

    depends_on = [
        aws_secretsmanager_secret.gps_app_azure_secret
    ]

    lifecycle {
      ignore_changes = [
        secret_string
      ]
    }
}

resource "aws_secretsmanager_secret_rotation" "gps_app_db_master_secret_rotation" {
    secret_id                   = aws_secretsmanager_secret.gps_app_db_master_secret.id 
    rotation_lambda_arn         = aws_lambda_function.secret_rotation_lambda.arn 

    rotation_rules {
        automatically_after_days = 90
    }
}