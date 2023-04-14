import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { S3BucketStack } from '../lib/s3-bucket-stack';
import { basicLambdaStack } from '../lib/basic-lambda-stack';

const app = new cdk.App();

// Deploying basic Lambda function
const basic_lambda_stack = new basicLambdaStack(app, 'basicLambdaStack');

// Creating an S3 bucket stack
const s3_bucket_stack = new S3BucketStack(app, 'mtS3Stack', {
  lambdaFunction: basic_lambda_stack.lambdaFunction
});

// Re-using assets
const bucket = s3_bucket_stack.bucket;