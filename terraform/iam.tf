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
      "Resource": "${aws_s3_bucket.reddit_stevenhurwitt.arn}"
    }
  ]
}
EOT
}

