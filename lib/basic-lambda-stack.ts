import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';


export class basicLambdaStack extends cdk.Stack{
  // Making the object accessible for reuseability
  public readonly lambdaFunction: lambda.Function;

  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const function_name = 'mete-lambda';
    const lambda_path = 'src/lambda/basic_lambda';

    const layer = new lambda.LayerVersion(this, 'LibraryLayer', {
      code: lambda.Code.fromAsset('src/lambda/codes/python/python.zip'),
      description: 'Common helper utility',
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_8],
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
    // Initialization of the lambda function
    this.lambdaFunction = new lambda.Function(this, function_name, {
        functionName: function_name,
        runtime: lambda.Runtime.PYTHON_3_8,
        code: lambda.Code.fromAsset(lambda_path),
        timeout: cdk.Duration.minutes(5),
        handler: "lambda_function.lambda_handler",
        layers: [layer]
    });
  }
}