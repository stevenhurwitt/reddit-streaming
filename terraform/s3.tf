################### S3 #####################

resource "aws_s3_bucket" "reddit_stevenhurwitt" {
    bucket        = "${var.s3_bucket_name}"
}

resource "aws_s3_bucket_acl" "reddit_stevenhurwitt" {
    bucket        = aws_s3_bucket.reddit_stevenhurwitt.id
    acl         = "private"
}

# raw folders
resource "aws_s3_object" "news" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_news}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "technology" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_technology}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "worldnews" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_worldnews}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "ProgrammerHumor" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_ProgrammerHumor}/"
  content_type = "application/x-directory"
}

# clean folders
resource "aws_s3_object" "news_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_news_clean}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "technology_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_technology_clean}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "worldnews_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_worldnews_clean}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "ProgrammerHumor_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_ProgrammerHumor_clean}/"
  content_type = "application/x-directory"
}

# add more folders! (8 total per core)

# raw folders
resource "aws_s3_object" "blackpeopletwitter" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_blackpeopletwitter}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "bikinibottomtwitter" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_bikinibottomtwitter}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "whitepeopletwitter" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_whitepeopletwitter}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "aws" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_aws}/"
  content_type = "application/x-directory"
}

# clean folders
resource "aws_s3_object" "blackpeopletwitter_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_blackpeopletwitter_clean}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "whitepeopletwitter_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_whitepeopletwitter_clean}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "bikinibottomtwitter_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_bikinibottomtwitter_clean}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "aws_clean" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_aws_clean}/"
  content_type = "application/x-directory"
}

# jar folder
resource "aws_s3_object" "jars" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_jars}/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "scripts" {
  bucket       = "${aws_s3_bucket.reddit_stevenhurwitt.id}"
  key          = "${var.folder_scripts}/"
  content_type = "application/x-directory"
}