# access key & secret are env var's AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# all other var's are env var's TF_VARS_<var name>

# variable "aws_access_key" {
#   type = string
# }

# variable "aws_secret_key" {
#   type = string
# }

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

variable "folder_jars" {
    default = "jars"
    type = string
}

variable "folder_scripts" {
    default = "scripts"
    type = string
}