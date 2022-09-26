# access key & secret are env var's AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# all other var's are env var's TF_VARS_<var name>

variable "aws_access_key" {
  type = string
}

variable "aws_secret_key" {
  type = string
}

variable "aws_region" {
    default = "us-east-2"
    type = string
}

variable "s3_bucket_name" {
    default = "reddit-streaming-stevenhurwitt"
    type = string
}

variable "folder_news" {
    default = "news"
    type = string
}

variable "folder_technology" {
    default = "technology"
    type = string
}

variable "folder_ProgrammerHumor" {
    default = "ProgrammerHumor"
    type = string
}

variable "folder_worldnews" {
    default = "worldnews"
    type = string
}

variable "folder_news_clean" {
    default = "news_clean"
    type = string
}

variable "folder_technology_clean" {
    default = "technology_clean"
    type = string
}

variable "folder_ProgrammerHumor_clean" {
    default = "ProgrammerHumor_clean"
    type = string
}

variable "folder_worldnews_clean" {
    default = "worldnews_clean"
    type = string
}

# more subreddits

variable "folder_blackpeopletwitter" {
    default = "blackpeopletwitter"
    type = string
}

variable "folder_whitepeopletwitter" {
    default = "whitepeopletwitter"
    type = string
}

variable "folder_bikinibottomtwitter" {
    default = "bikinibottomtwitter"
    type = string
}

variable "folder_aws" {
    default = "aws"
    type = string
}

variable "folder_blackpeopletwitter_clean" {
    default = "blackpeopletwitter_clean"
    type = string
}

variable "folder_whitepeopletwitter_clean" {
    default = "whitepeopletwitter_clean"
    type = string
}

variable "folder_bikinibottomtwitter_clean" {
    default = "bikinibottomtwitter_clean"
    type = string
}

variable "folder_aws_clean" {
    default = "aws_clean"
    type = string
}

variable "folder_jars" {
    default = "jars"
    type = string
}

variable "folder_scripts" {
    default = "scripts"
    type = string
}

variable "vault_uri" {
    description = "Target address for Vault."
    type = string
}

variable "vault_username" {
    description = "Username for Vault authentication."
    type = string
}

variable "vault_password" {
    description = "Password for Vault authentication."
    type = string
}

variable "vault_config_path" {
    description = "The path for Terraform to check in Vault for tfvars. This is declared in Vault should look something like concourse/aws-core/{your-pipeline-name}"
    type = string
}

variable "aws_region" {
    description = "AWS region to which PAS will be deployed."
    type = string
    default = "us-east-2"
}

variable "docker_tag" {
    description = "docker tag for deployment from ecr"
    type = string
}

variable "desired_task_number" {
    description = "Desired task number in ECS service"
    type = number
    default = 1
}