#################### s3 ####################
resource "aws_iam_policy" "s3_policy" {
  name        = "s3-policy"
  description = "My test policy"

  policy = <<EOT
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": "${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}"
    }
  ]
}
EOT
}

#################### glue ####################
resource "aws_iam_role" "glue" {
  name = "AWSGlueServiceRoleDefault"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service":  [
                      "glue.amazonaws.com",
                      "events.amazonaws.com",
                      "lambda.amazonaws.com
                    ]
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "glue_service" {
    role = aws_iam_role.glue.id
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "my_s3_policy" {
  name = "my_s3_policy"
  role = aws_iam_role.glue.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::${var.s3_bucket_name}",
        "arn:aws:s3:::${var.s3_bucket_name}/*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "my_athena_policy" {
  name = "my_athena_policy"
  role = "${aws_iam_role.glue.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:*"
      ],
      "Resource": [
        "*"
      ]
    }
  ]
}
EOF
}

################### lambda ###################
resource "aws_iam_role" "gps_lambda_secret_rotation_role" {
  name                  = "${local.product}-lambda-secret-rotation-role${local.environment}"
  assume_role_policy    = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": "lambda.amazonaws.com"
        },

        "Sid": ""
      }
    ]
  }
EOF
  tags                  = merge(local.product_tags, map("name", "lambda-iam-role"))
}

resource "aws_iam_role_policy" "gps_lambda_secret_rotation_role_policy" {
  role    = aws_iam_role.gps_lambda_secret_rotation_role.id
  policy  = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": ["arn:aws:logs:*:*:log-group:/aws/lambda/${local.product}-*"]
      },
      {
        "Sid": "SecretsManagerAccess",
        "Effect": "Allow",
        "Action": [
          "secretsmanager:DescribeSecret",
          "secretsmanager:GetSecretValue",
          "secretsmanager:PutSecretValue",
          "secretsmanager:UpdateSecretVersionStage"
        ],
        "Resource": "arn:aws:secretsmanager:*:*:secret:${local.product}-*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "secretsmanager:GetRandomPassword"
        ],
        "Resource": "*"
      },
      {
        "Action": [
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces"
        ],
        "Resource": "*",
        "Effect": "Allow"
      },
      {
        "Sid": "RDSDataServiceAccess",
        "Effect": "Allow",
        "Action": [
          "rds-data:BatchExecuteStatement",
          "rds-data:BeginTransaction",
          "rds-data:CommitTransaction",
          "rds-data:ExecuteStatement",
          "rds-data:RollbackTransaction"
        ],
        "Resource": "arn:aws:rds:${var.aws_region}:*:cluster:${local.product}-*"
      }
    ]
  }
  EOF
}

# delete project role
resource "aws_iam_role" "gps_daily_delete_role" {
  name = "${local.product}-daily-delete${local.environment}-lambda-policy"
  assume_role_policy = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": [
            "lambda.amazonaws.com",
            "events.amazonaws.com"
          ]
        },
        "Effect": "Allow",
        "Sid": ""
      }
    ]
  }
EOF
  tags = merge(local.product_tags, map("name", "gps-daily-delete-iam-role"))
}

resource "aws_iam_role_policy" "gps_daily_delete_policy" {
  role = aws_iam_role.gps_daily_delete_role.id
  policy = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "events:PutRule"
        ],
        "Resource": "arn:aws:events:${var.aws_region}:${local.aws_account_id}:*/${local.product}*"
      }
    ]
  }
EOF
}