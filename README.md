# Customer Order Summary

This project calculates the total amount spent and the number of orders for each customer from a given set of data. The input data consists of three CSV files: customers, orders, and items.

## Overview

The purpose of this project is to analyze customer order data and generate a summary for each customer. The summary includes:

- The total amount spent by each customer
- The number of orders placed by each customer

Additionally, the program identifies any errors in the data, such as missing or mismatched customer and order references, and outputs error messages accordingly.

## Input Data

The input data is expected to be in three separate CSV files:

1. **customers.csv:** Contains customer data with the following columns:
   - `customer_reference`: Unique identifier for the customer
   - Other customer-related information (e.g., name, address, etc.)

2. **orders.csv:** Contains order data with the following columns:
   - `order_reference`: Unique identifier for the order
   - `customer_reference`: The customer who placed the order (must match a customer in customers.csv)
   - Other order-related information (e.g., order date, status, etc.)

3. **items.csv:** Contains item data with the following columns:
   - `order_reference`: The order to which the item belongs (must match an order in orders.csv)
   - `total_price`: The price of the item
   - Other item-related information (e.g., product name, quantity, etc.)

## Output

The program generates two JSON arrays:

1. **Customer Messages:** A JSON array containing a summary for each customer, including the total amount spent and the number of orders. Each entry in the array has the following format:

   ```json
   {
     "customer_reference": "customer_id",
     "order_count": number_of_orders,
     "total_price": total_amount_spent,
     "type": "customer_message"
   }

2. **Error Messages*:** A JSON array containing error messages for any issues found in the data, such as missing or mismatched customer and order references. Each entry in the array has the following format:

   ```json
   {
     "type": "error_message",
     "customer_reference": null_or_customer_id,
     "order_reference": order_id,
     "message": "Error description"
   }


## Export Libraries

Create a new folder on your shell to hold the Python libraries you want to use in your Lambda function.
   mkdir my-lambda-layer
   cd my-lambda-layer

Next, install the Python libraries you need using pip3.8. The -t option specifies the target directory where the libraries will be installed.
   pip3.8 install <python libraries> -t .

Zip the Folder
   Once the libraries are installed, zip the contents of the folder.
   zip -r my-lambda-layer.zip .


## Implementation

The project is implemented in both JavaScript (Node.js) and Python. The main logic is the same for both implementations:

1. Read the input data from the CSV files.
2. Merge the data from the orders and customers files based on the customer_reference.
3. Merge the data from the orders and items files based on the order_reference.
4. Calculate the total amount spent and the number of orders for each customer.
5. Generate the customer summary and error messages.
6. Print the output JSON arrays.
7. You need to add Python libraries to your AWS Lambda. To do that read the Export Libraries Section

The project also includes publishing the customer messages and error messages using message queuing protocols such as AMQP, MQTT, and SQS. You can find the code for this in the /src/codes/ directory.

## Deployment

To deploy this project, follow these steps:

1. Run the `aws configure` command and configure your local PC for AWS. This will involve providing your AWS access key ID, secret access key, default region, and output format. This will allow your local machine to interact with AWS resources.
2. Once your local machine is configured for AWS, run `npm install` to install the necessary dependencies.
3. Run `npm run build` to build the project. This will create a build folder that contains the compiled JavaScript code.
4. Run `cdk synth` to generate a CloudFormation template that describes the AWS resources that need to be created for this project.
5. Run `cdk deploy s3bucket-mete sqs-mete` to deploy the AWS resources defined in the CloudFormation template. This will create an S3 bucket with Lambda and an SQS queue.
6. In your Lambda function code, you can choose which message queue stack to use: AMQP, MQTT, or SQS. You will need to configure your Lambda function to read from the appropriate message queue stack.
7. Publish the customer messages and error messages to the message queue stack of your choice. The customer messages should be in the following format:

    {
        "customer_reference": "<customer_reference>",
        "total_amount_spent": <total_amount_spent>,
        "number_of_orders": <number_of_orders>
    }

8. The error messages should be in the following format:

    {
        "customer_reference": "<customer_reference>",
        "error_message": "<error_message>"
    }

And that's it! Once you've completed these steps, you should be able to send messages to the message queue and have them processed by the Lambda function.
