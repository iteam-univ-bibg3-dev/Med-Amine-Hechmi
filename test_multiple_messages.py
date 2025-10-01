import pika
import json
import os
import time
from elasticsearch import Elasticsearch

# ===============================
# Configuration
# ===============================
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")  # depuis host Windows
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "localhost")    # depuis host Windows
INDEX_NAME = "test-index"
NUM_MESSAGES = 5  # nombre de messages à envoyer
DELAY_AFTER_SEND = 3  # secondes d'attente avant vérification Elasticsearch

# ===============================
# Connexion RabbitMQ
# ===============================
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue="test-queue", durable=True)
print("✅ Connecté à RabbitMQ")

# ===============================
# Connexion Elasticsearch
# ===============================
es = Elasticsearch([f"http://{ELASTIC_HOST}:9200"])
while not es.ping():
    print("Elasticsearch non prêt, attente 5s...")
    time.sleep(5)
print("✅ Connecté à Elasticsearch")

# ===============================
# Envoyer plusieurs messages
# ===============================
messages = []
for i in range(1, NUM_MESSAGES + 1):
    msg = {"user": f"user{i}", "action": "test", "value": i*10}
    messages.append(msg)
    channel.basic_publish(
        exchange='',
        routing_key='test-queue',
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2)  # persistant
    )
    print(f"📤 Message envoyé : {msg}")

connection.close()

# ===============================
# Attendre que le worker consomme les messages
# ===============================
print(f"⏱ Attente {DELAY_AFTER_SEND} secondes pour que le worker traite les messages...")
time.sleep(DELAY_AFTER_SEND)

# ===============================
# Vérifier les messages dans Elasticsearch
# ===============================
res = es.search(index=INDEX_NAME, body={"query": {"match_all": {}}})
hits = res['hits']['hits']

print(f"🔍 {len(hits)} documents trouvés dans Elasticsearch :")
for hit in hits:
    print(hit['_source'])

# Vérification simple
if len(hits) >= NUM_MESSAGES:
    print("✅ Tous les messages ont été indexés avec succès !")
else:
    print("⚠️ Certains messages n'ont pas encore été indexés.")
