import boto3

sqs = boto3.resource('sqs')
queue_url = ''

while True:
    messages = sqs.Queue(queue_url).receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5)
    
    if not messages:
        print('No messages in queue')
        continue
    
    for message in messages:
        print(message.body)
        message.delete()