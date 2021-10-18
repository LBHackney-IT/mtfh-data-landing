
# INSTRUCTIONS:
# 1) ENSURE YOU POPULATE THE LOCALS
# 2) ENSURE YOU REPLACE ALL INPUT PARAMETERS, THAT CURRENTLY STATE 'ENTER VALUE', WITH VALID VALUES
# 3) YOUR CODE WOULD NOT COMPILE IF STEP NUMBER 2 IS NOT PERFORMED!
# 4) ENSURE YOU CREATE A BUCKET FOR YOUR STATE FILE AND YOU ADD THE NAME BELOW - MAINTAINING THE STATE OF THE INFRASTRUCTURE YOU CREATE IS ESSENTIAL - FOR APIS, THE BUCKETS ALREADY EXIST
# 5) THE VALUES OF THE COMMON COMPONENTS THAT YOU WILL NEED ARE PROVIDED IN THE COMMENTS
# 6) IF ADDITIONAL RESOURCES ARE REQUIRED BY YOUR API, ADD THEM TO THIS FILE
# 7) ENSURE THIS FILE IS PLACED WITHIN A 'terraform' FOLDER LOCATED AT THE ROOT PROJECT DIRECTORY

terraform {
    required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 3.0"
        }
    }
}

provider "aws" {
    region = "eu-west-2"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
    parameter_store = "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter"
}

terraform {
  backend "s3" {
    bucket  = "terraform-state-housing-development"
    encrypt = true
    region  = "eu-west-2"
    key     = "services/mtfh-data-landing/state"
  }
}

resource "aws_s3_bucket" "bucket_landing_data" {
  bucket = "mtfh-data-landing-spike"
  acl    = "private"
}

data "aws_ssm_parameter" "person_sns_topic_arn" {
  name = "/sns-topic/development/person/arn"
}

data "aws_ssm_parameter" "tenure_sns_topic_arn" {
  name = "/sns-topic/development/tenure/arn"
}

resource "aws_sqs_queue" "mtfh_data_landing_dead_letter_queue" {
  name                        = "mtfhdatalandingdeadletterqueue.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  kms_master_key_id           = "alias/housing-development-cmk"
  kms_data_key_reuse_period_seconds = 300
}

resource "aws_sqs_queue" "mtfh_data_landing_queue" {
  name                        = "mtfhdatalandingqueue.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  kms_master_key_id           = "alias/housing-development-cmk"
  kms_data_key_reuse_period_seconds = 300
  redrive_policy              = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.mtfh_data_landing_dead_letter_queue.arn,
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_policy" "mtfh_data_landing_queue_policy" {
  queue_url = aws_sqs_queue.mtfh_data_landing_queue.id
  policy = <<POLICY
  {
      "Version": "2012-10-17",
      "Id": "sqspolicy",
      "Statement": [
      {
          "Sid": "First",
          "Effect": "Allow",
          "Principal": "*",
          "Action": "sqs:SendMessage",
          "Resource": "${aws_sqs_queue.mtfh_data_landing_queue.arn}",
          "Condition": {
          "ArnEquals": {
              "aws:SourceArn": "${data.aws_ssm_parameter.person_sns_topic_arn.value}"
          }
          },
          {
              "Sid": "Second",
              "Effect": "Allow",
              "Principal": "*",
              "Action": "sqs:SendMessage",
              "Resource": "${aws_sqs_queue.mtfh_data_landing_queue.arn}",
              "Condition": {
              "ArnEquals": {
                  "aws:SourceArn": "${data.aws_ssm_parameter.tenure_sns_topic_arn.value}"
              }
              }
          },  
      }
      ]
  }
  POLICY
}

resource "aws_sns_topic_subscription" "mtfh_data_landing_queue_subscribe_to_person_sns" {
  topic_arn = data.aws_ssm_parameter.person_sns_topic_arn.value
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.mtfh_data_landing_queue.arn
  raw_message_delivery = true
}

resource "aws_sns_topic_subscription" "person_queue_subscribe_to_tenure_sns" {
  topic_arn            = data.aws_ssm_parameter.tenure_sns_topic_arn.value
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.mtfh_data_landing_queue.arn
  raw_message_delivery = true
}

resource "aws_ssm_parameter" "mtfh_data_landing_sqs_queue_arn" {
  name  = "/sqs-queue/development/mtfh_data_landing/arn"
  type  = "String"
  value = aws_sqs_queue.mtfh_data_landing_queue.arn
}
