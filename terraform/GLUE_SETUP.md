# Terraform Configuration Summary

## New Files Created

### 1. `secrets.tf`
Creates AWS Secrets Manager secrets for storing AWS credentials:
- `AWS_ACCESS_KEY_ID` - Stored securely in Secrets Manager
- `AWS_SECRET_ACCESS_KEY` - Stored securely in Secrets Manager

The Glue jobs retrieve these credentials at runtime using the AWS Secrets Manager SDK.

### 2. `glue_jobs_v2.tf`
Defines modernized Glue jobs with:
- **Glue Version**: 4.0 (latest, supports Spark 3.5+ and Delta Lake 3.2+)
- **Worker Type**: G.2X (improved from G.1X)
- **Worker Count**: 2 (configurable)
- **Python Version**: 3
- **Timeout**: 60 minutes
- **Retries**: 1 attempt

Each job is configured with:
- CloudWatch logging enabled
- Spark UI enabled
- Environment variable: `BUCKET_NAME` (passed to scripts via --BUCKET_NAME)
- Proper IAM role for S3 and Secrets Manager access

Jobs created:
- `technology-curation-v2`
- `news-curation-v2`
- `ProgrammerHumor-curation-v2`
- `worldnews-curation-v2`

### 3. `glue_schedules.tf`
Creates EventBridge Scheduler rules for daily execution at midnight UTC:

Each schedule:
- Uses cron expression: `cron(0 0 * * ? *)` (midnight UTC every day)
- Has proper IAM role to invoke Glue jobs
- Includes flexible time window setting

Schedules created:
- `technology-curation-midnight`
- `news-curation-midnight`
- `programmerhumer-curation-midnight`
- `worldnews-curation-midnight`

## Updated Files

### `iam.tf`
Added new IAM policy `glue_secrets_policy` that allows Glue role to:
- Read secrets from Secrets Manager
- Describe secrets

## Deployment Instructions

1. **Upload scripts to S3**:
   ```bash
   aws s3 cp redditStreaming/src/scripts/*-curation.py \
     s3://reddit-streaming-stevenhurwitt-2/scripts/
   ```

2. **Initialize and apply Terraform**:
   ```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

3. **Verify deployment**:
   - Check AWS Secrets Manager for the two new secrets
   - Verify Glue jobs in AWS console (look for `-v2` suffix)
   - Verify EventBridge Scheduler rules exist
   - Check CloudWatch for job execution logs

## Notes

- The old Glue jobs (without `-v2` suffix) are still in `glue.tf`. You can remove them once you verify the v2 jobs work.
- The `BUCKET_NAME` environment variable is passed to scripts. Update your Python scripts to use `os.environ.get("BUCKET_NAME", "default-bucket")` if needed.
- All jobs run at **midnight UTC**. Adjust the cron expression in `glue_schedules.tf` if you need a different time.
- Jobs have 60-minute timeout and 1 retry on failure.
- Secrets are stored with immediate deletion (no recovery window) for dev environments.

## Environment Variables Required

```bash
export TF_VARS_aws_access_key="your-aws-access-key"
export TF_VARS_aws_secret_key="your-aws-secret-key"
export TF_VARS_s3_bucket_name="your-bucket-name"
export TF_VARS_aws_region="us-east-2"
```

Or set them in a `.tfvars` file:
```hcl
aws_access_key = "your-key"
aws_secret_key = "your-secret"
s3_bucket_name = "reddit-streaming-stevenhurwitt-2"
aws_region     = "us-east-2"
```
