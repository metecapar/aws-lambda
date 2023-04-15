import datetime
from io import StringIO
import json
import boto3
import logging
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    # Outputs the incoming event into CW logs
    logger.info("Event:")
    logger.info(event)

    # Get the current time and format the date
    file_date = datetime.datetime.now().strftime("%d%m%Y")

    # Define file names
    customer_file = f"customers_{file_date}.csv"
    orders_file = f"orders_{file_date}.csv"
    items_file = f"items_{file_date}.csv"

    s3 = boto3.client('s3')
    bucketName = 'mts3stack-bucket'
    getCustomerCSV = s3.get_object(Bucket=bucketName, Key=customer_file)
    getOrdersCSV = s3.get_object(Bucket=bucketName, Key=orders_file)
    getItemsCSV = s3.get_object(Bucket=bucketName, Key=items_file)

    customers = StringIO(getCustomerCSV['Body'].read().decode('utf-8'))
    orders = StringIO(getOrdersCSV['Body'].read().decode('utf-8'))
    items = StringIO(getItemsCSV['Body'].read().decode('utf-8'))

    print(type(customers))
    try:
        # Read CSV files
        customers_df = pd.read_csv(customers)
        orders_df = pd.read_csv(orders)
        items_df = pd.read_csv(items)
    except:
        logger.error("Error while reading csv's")
    # Validation
    missing_customer_references = orders_df[~orders_df["customer_reference"].isin(
        customers_df["customer_reference"])]
    missing_customer_reference_errors = missing_customer_references.copy()
    missing_customer_reference_errors["type"] = "error_message"
    missing_customer_reference_errors["message"] = "Customer reference not found in customers."
    missing_customer_reference_errors = missing_customer_reference_errors[[
        "type", "customer_reference", "order_reference", "message"]]

    # Merge DataFrames
    orders_items_df = orders_df.merge(
        items_df, on="order_reference", how="inner")
    orders_customers_df = orders_items_df.merge(
        customers_df, on="customer_reference", how="inner")

    # Calculate total amount spent and number of orders per customer
    total_amount_spent = (
        orders_customers_df.groupby("customer_reference")[
            "total_price"].sum().reset_index()
    )
    number_of_orders = (
        orders_customers_df.groupby("customer_reference")["order_reference"]
        .nunique()
        .reset_index()
    )

    # Create customer summary
    customer_summary = number_of_orders.merge(
        total_amount_spent, on="customer_reference")
    customer_summary["type"] = "customer_message"

    # Check for not found order references in items
    missing_order_references = items_df[~items_df["order_reference"].isin(
        orders_df["order_reference"])]
    missing_order_reference_errors = missing_order_references.copy()
    missing_order_reference_errors["type"] = "error_message"
    missing_order_reference_errors["customer_reference"] = None
    missing_order_reference_errors["message"] = "Order reference not found in orders."
    missing_order_reference_errors = missing_order_reference_errors[[
        "type", "customer_reference", "order_reference", "message"]]

    # Combine error messages
    all_error_messages = pd.concat(
        [missing_customer_reference_errors, missing_order_reference_errors])

    # Print results
    print(customer_summary.to_json(orient="records"))
    print(all_error_messages.to_json(orient="records"))

    # Publish results to Amazon SQS
    sqs = boto3.resource('sqs')
    queue_url = ''

    # Send customer messages
    customer_messages = customer_summary.to_dict(orient='records')
    for message in customer_messages:
        sqs_message = {
            'Id': str(message['customer_reference']),
            'MessageBody': json.dumps(message)
        }
        sqs_response = sqs.meta.client.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(sqs_message))

    # Send error messages
    error_messages = all_error_messages.to_dict(orient='records')
    for message in error_messages:
        sqs_message = {
            'Id': str(message['order_reference']),
            'MessageBody': json.dumps(message)
        }
        sqs_response = sqs.meta.client.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(sqs_message))

    return {
        "statusCode": 200,
        "sqsSend": True
    }


# For direct invocation and testing on the local machine
if __name__ == '__main__':
    print(lambda_handler(None, None))
