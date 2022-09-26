resource "aws_rds_cluster" "gps_mysql_cluster" {
    cluster_identifier = "${local.product}-aurora-mysql${local.environment}"
    engine = "aurora-mysql"
    engine_mode = "serverless"
    enable_http_endpoint = true 
    availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]
    master_username = local.db_master_username
    master_password = local.db_master_password
    database_name = local.db_name
    db_subnet_group_name = local.db_subnet_group_name
    db_cluster_parameter_group_name = "default.aurora-mysql5.7"
    vpc_security_group_ids = [data.aws_security_group.gps_rds_sg.id]
    apply_immediately = true 
    deletion_protection = local.aurora_delete_protection
    skip_final_snapshot = true 
    backup_retention_period = local.backup_retention_period
    storage_encrypted = true 

    scaling_configuration {
        auto_pause          = local.aurora_auto_pause
        min_capacity        = local.aurora_min_capacity
        max_capacity        = local.aurora_max_capacity
    }

    tags = merge(local.standard_tags, map("name", "mysql-cluster"))
}