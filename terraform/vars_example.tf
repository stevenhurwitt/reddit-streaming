variable "availability_zone_names" {
  type    = list(string)
  default = ["us-east-2a"]
}

variable "aws_client" {
  type = string
  default = ""
}

variable "aws_secret" {
  type = string
  default = ""
}