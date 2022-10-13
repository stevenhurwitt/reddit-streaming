##################### lambda #####################
# data "archive_file" "dummy" {
#     type="zip"
#     output_path="${path.module}/lambda_funcion_payload.zip"
#     source {
#         content="dummy"
#         filename="dummy.txt"
#     }
# }

# # secret rotation lambda
# resource "aws_lambda_function" "secret_rotation_lambda" {
#     function_name           = "${local.product}-db-secret-rotation${local.environment}"
#     role                    = aws_iam_role.gps_lambda_secret_rotation_role.arn
#     filename                = data.archive_file.dummy.output_path
#     handler                 = "secret_rotation_handler.rotate_db_secret"
#     runtime                 = "python3.9"
#     description             = "Rotate aurora db secret"
#     timeout                 = local.lambda_timeout
#     environment {
#         variables = {
#             SECRETS_MANAGER_ENDPOINT = "https://secretsmanager.${var.aws_region}.amazonaws.com"
#         }
#     }

#     vpc_config {
#         subnet_ids = data.aws_subnet_ids.private_app_subnets.ids 
#         security_group_ids = [data.aws_security_group.gps_rds_sg.id]
#     }

#     tags                    = merge(local.product_tags, map("name", "rotate-secret-lambda"))
# }

# # delete project lambda
# resource "aws_lambda_function" "delete_project_lambda" {
#     function_name           = "${local.product}-delete-project${local.environment}"
#     role                    = aws_iam_role.gps_lambda_secret_rotation_role.arn
#     filename                = data.archive_file.dummy.output_path
#     handler                 = "secret_rotation_handler.rotate_db_secret"
#     runtime                 = "python3.9"
#     description             = "Rotate aurora db secret"
#     timeout                 = local.lambda_timeout
#     environment {
#         variables = {
#             SECRETS_MANAGER_ENDPOINT = "https://secretsmanager.${var.aws_region}.amazonaws.com"
#         }
#     }

#     vpc_config {
#         subnet_ids = data.aws_subnet_ids.private_app_subnets.ids 
#         security_group_ids = [data.aws_security_group.gps_rds_sg.id]
#     }

#     tags                    = merge(local.product_tags, map("name", "rotate-secret-lambda"))
# }

# # cloudwatch events
# resource "aws_lambda_permission" "allow_cloudwatch" {
#     statement_id = "AllowExecutionFromCloudwatch"
#     action = "lambda:InvokeFunction"
#     function_name = aws_lambda_function.delete_project_lambda.function_name
#     principal = "events.amazonaws.com"
#     source_arn = "arn:aws:events:${var.aws_region}:${local.aws_account_id}:*/${local.product}*"
# }