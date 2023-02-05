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
      "Resource": [
        "${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}/*",
        "${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}"
        ]
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
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "glue_policy" {
  role=aws_iam_role.glue.id
  policy=<<EOF
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
        "s3:ListBucket",
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:DeleteObject"
      ],
      "Resource": ${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": "${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey",
        "kms:ReEncrypt",
        "kms:Describe"
      ],
      "Resource": "${aws_kms_alias.rds_cmk_alias.arn}/*"wbnnnnnnnbn     4444444444444444444444444444444
    },
  ]
}
EOF
}

# resource "aws_iam_role_policy_attachment" "glue_service" {
#     role = "${aws_iam_role.glue.id}"
#     policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
# }

resource "aws_iam_role_policy" "my_s3_policy" {
  name = "my_s3_policy"
  role = "${aws_iam_role.glue.id}"
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
        "${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}",
        "${aws_s3_bucket.reddit_streaming_stevenhurwitt.arn}/*"
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