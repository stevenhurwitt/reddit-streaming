provider "vault" {
    address = var.vault_url
    skip_tls_verify = true
    auth_login {
        path = "auth/userpass/login/${var.vault_username}"
        parameters = {
            password = var.vault_password
        }
    }
}

data "vault_generic_secret" "config" {
    path = "${var.vault_config_path}/tfvars"
}

data "vault_generic_secret" "product_tags" {
    path = "${var.vault_config_path}/product-tags"
}

data "vault_generic_secret" "container_config" {
    path = "${var.vault_config_path}/container-config"
}

data "vault_generic_secret" "azure_ad_config" {
    path = "${var.vault_config_path}/azure-ad-config"
}

data "vault_generic_secret" "db_master_secrets" {
    path = "${var.vault_config_path}/db-master-secret"
}

data "vault_generic_secret" "aws_account_id" {
    path = "${var.vault_config_path}/aws-account-id"
}