import datetime
from io import StringIO
import json
import boto3
import logging
import time
import csv
import pika
import paho.mqtt.client as mqtt
from collections import defaultdict

messageQueueType = 'sqs'


def amqpSender(status, message):
    user = ""
    password = ""
    host = ""
    port = ""
    virtual_host = ""

    # Connect to RabbitMQ
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host, port, virtual_host, credentials))

    channel = connection.channel()

    # Declare a queue
    channel.queue_declare(queue='data_queue')
    if status == 1:
        channel.basic_publish(
            exchange='', routing_key='data_queue', body=json.dumps(message))
    else:
        channel.basic_publish(
            exchange='', routing_key='data_queue_error', body=json.dumps(message))


def mqttSender(status, record):
    # Define MQTT settings
    MQTT_BROKER_HOST = "mqtt.example.com"
    MQTT_BROKER_PORT = 1883
    MQTT_CLIENT_ID = "csv_processor"
    MQTT_USER = ""
    MQTT_PASS = ""
    MQTT_TOPIC_CUSTOMER_MESSAGES = "customer_messages"
    MQTT_TOPIC_ERROR_MESSAGES = "error_messages"

    # Create MQTT client instance
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    if status == 1:
        mqtt_client.publish(MQTT_TOPIC_CUSTOMER_MESSAGES, json.dumps(record))
        time.sleep(0.1)
    else:
        mqtt_client.publish(MQTT_TOPIC_ERROR_MESSAGES, json.dumps(record))
        time.sleep(0.1)


def sqsSender(status, messageSQS):
    # Publish results to Amazon SQS
    sqs = boto3.resource('sqs')
    queue_url = 'https://sqs.us-west-1.amazonaws.com/813334080301/sqs-mete-meteparserqueueE077929E-IrKAPUgO1Kvb'

    if status == 1:
        sqs_message = {
            'Id': str(messageSQS['customer_reference']),
            'MessageBody': json.dumps(messageSQS)
        }
        sqs_response = sqs.meta.client.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(sqs_message))
    else:
        sqs_message = {
            'Id': str(messageSQS['order_reference']),
            'MessageBody': json.dumps(messageSQS)
        }
        sqs_response = sqs.meta.client.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(sqs_message))


def read_csv_from_s3(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = StringIO(response['Body'].read().decode('utf-8'))
        reader = csv.DictReader(content)
        return [row for row in reader]
    except:
        logger.error("Error while reading from s3")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    # Outputs the incoming event into CW logs
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
                customer_summary[customer_ref]['total_price'] += float(
                    item["total_price"])

    customer_messages = [{
        "type": "customer_message",
        "customer_reference": k,
        "orders": v["orders"],
        "total_price": v["total_price"]
    } for k, v in customer_summary.items()]

    print(json.dumps(customer_messages, indent=2))
    print(json.dumps(all_error_messages, indent=2))

    # Send customer messages
    for message in customer_messages:
        if messageQueueType == 'sqs':
            sqsSender(1, message)
        elif messageQueueType == 'amqp':
            amqpSender(1, message)
        elif messageQueueType == 'mqtt':
            mqttSender(1, message)
    # Send error messages
    for message in all_error_messages:
        if messageQueueType == 'sqs':
            sqsSender(0, message)
        elif messageQueueType == 'amqp':
            amqpSender(0, message)
        elif messageQueueType == 'mqtt':
            mqttSender(0, message)

    return {
        "statusCode": 200,
        "sqsSend": True
    }


# For direct invocation and testing on the local machine
if __name__ == '__main__':
    print(lambda_handler(None, None))
