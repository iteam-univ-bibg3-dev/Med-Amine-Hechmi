import pika
import json
import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

# Connexion à RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue="test-queue", durable=True)

# Exemple de message
message = {
    "user": "amine",
    "action": "test",
    "value": 123
}

channel.basic_publish(
    exchange='',
    routing_key='test-queue',
    body=json.dumps(message),
    properties=pika.BasicProperties(
        delivery_mode=2,  # persistent
    )
)

print(f"📤 Message envoyé : {message}")
connection.close()
