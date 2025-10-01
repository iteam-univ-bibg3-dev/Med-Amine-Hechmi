import pika
import json
import os
import time
import random

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "test-queue"

# Connexion RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)
print("✅ Connecté à RabbitMQ, envoi de messages en continu...")

users = ["alice", "bob", "carol", "dave", "eve"]

try:
    while True:
        msg = {
            "user": random.choice(users),
            "action": "test",
            "value": random.randint(1, 100)
        }
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(msg),
            properties=pika.BasicProperties(delivery_mode=2)  # persistant
        )
        print(f"📤 Message envoyé : {msg}")
        time.sleep(1)  # envoie un message par seconde
except KeyboardInterrupt:
    print("⏹ Envoi arrêté par l'utilisateur")
    connection.close()
