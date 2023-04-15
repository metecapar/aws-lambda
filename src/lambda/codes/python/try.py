import csv
import json
import boto3
import logging
from io import StringIO
import datetime
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def read_csv_from_s3(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = StringIO(response['Body'].read().decode('utf-8'))
    reader = csv.DictReader(content)
    return [row for row in reader]


def lambda_handler(event, context):
    logger.info("Event:")
    logger.info(event)

    file_date = datetime.datetime.now().strftime("%d%m%Y")

    customer_file = f"customers_{file_date}.csv"
    orders_file = f"orders_{file_date}.csv"
    items_file = f"items_{file_date}.csv"

    s3 = boto3.client('s3')
    bucketName = 'mete-bucket-55'

    customers = read_csv_from_s3(s3, bucketName, customer_file)
    orders = read_csv_from_s3(s3, bucketName, orders_file)
    items = read_csv_from_s3(s3, bucketName, items_file)

    customer_dict = {c["customer_reference"]: c for c in customers}
    order_dict = {o["order_reference"]: o for o in orders}
    order_item_dict = defaultdict(list)

    for item in items:
        order_item_dict[item["order_reference"]].append(item)

    missing_customer_errors = []
    for order in orders:
        if order["customer_reference"] not in customer_dict:
            error_message = {
                "type": "error_message",
                "customer_reference": order["customer_reference"],
                "order_reference": order["order_reference"],
                "message": "Customer reference not found in customers."
            }
            missing_customer_errors.append(error_message)

    missing_order_errors = []
    for item in items:
        if item["order_reference"] not in order_dict:
            error_message = {
                "type": "error_message",
                "customer_reference": None,
                "order_reference": item["order_reference"],
                "message": "Order reference not found in orders."
            }
            missing_order_errors.append(error_message)

    all_error_messages = missing_customer_errors + missing_order_errors

    customer_summary = defaultdict(lambda: {'orders': 0, 'total_price': 0})

    for order in orders:
        customer_ref = order["customer_reference"]
        if customer_ref in customer_dict:
            customer_summary[customer_ref]['orders'] += 1
            for item in order_item_dict[order["order_reference"]]:
                customer_summary[customer_ref]['total_price'] += float(item["total_price"])

    customer_messages = [{
        "type": "customer_message",
        "customer_reference": k,
        "orders": v["orders"],
        "total_price": v["total_price"]
    } for k, v in customer_summary.items()]

    print(json.dumps(customer_messages, indent=2))
    print(json.dumps(all_error_messages, indent=2))

    sqs = boto3.resource('sqs')
    queue_url = ''

    # Send customer messages
    for message in customer_messages:
        sqs_message = {
            'Id': str(message['customer_reference']),
            'MessageBody': json.dumps(message)
        }
        sqs_response = sqs.meta.client.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(sqs_message))

    # Send error messages
    for message in all_error_messages:
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
