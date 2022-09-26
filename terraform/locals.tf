locals {
    product_tags = tomap(data.vault_generic_secret.product_tags.data)

    product = data.vault_generic_secret.product_tags.data["product"]
    business_unit = data.vault_generic_secret.product_tags.data["business_unit"]
    env = data.vault_generic_secret.config.data["env"]
    environment = data.vault_generic_secret.config.data["env_suffix"]
    bucket_prefix = data.vault_generic_secret.config.data["bucket_prefix"]
    standard_tags = tomap(data.vault_generic_secret.product_tags.data, local.product_tags)
    aws_account_id = data.vault_generic_secret.aws_account_id.data["value"]

    # aurora
    db_name = data.vault_generic_secret.db_master_secrets.data["dbname"]
    db_master_username = data.vault_generic_secret.db_master_secrets.data["username"]
    db_master_password = data.vault_generic_secret.db_master_secrets.data["password"]
    db_subnet_group_name = data.vault.generic_secret.db_master_secrets.data["gps_subnet_group_name"]
    aurora_min_capacity = data.vault.generic_secret.config.data["aurora_min_capacity"]
    aurora_max_capacity = data.vault.generic_secret.config.data["aurora_max_capacity"]
    backup_retention_period = data.vault.generic_secret.config.data["backup_retention_period"]
    db_endpoint = aws_rds_cluster.gps_mysql_cluster.endpoint
    nexus_tag = data.vault_generic_secret.config.data["nexus_tag"]
    aurora_delete_protection = data.vault_generic_secret.config.data["aurora_delete_protection"]

    # ecs
    ecs_cluster_name = "${local.product}-fargate-cluster"
    docker_container_port = data.vault_generic_secret.container_config.data["port"]
    ecs_service_name = "${local.product}-service"
    cpu = data.vault_generic_secret.container_config.data["cpu"]
    memory = data.vault_generic_secret.container_config.data["memory"]
    ecs_logs_retention_in_days = data.vault_generic_secret.container_config.data["ecs_logs_retention_in_days"]
    health_check_grace_period = data.vault_generic_secret.container_config.data["health_check_grace_period"]
    admin_email = data.vault_generic_secret.container_config.data["admin_email"]

    # lambda
    lambda_timeout = data.vault_generic_secret.config.data["lambda_timeout"]
}