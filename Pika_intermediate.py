import pika
import json
import socket

# RabbitMQ setup
queue_name = "medium_priority_disasters"
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.queue_declare(queue=queue_name, durable=True)

# Socket setup (acts as bridge to Spark Streaming)
TCP_IP = "localhost"
TCP_PORT = 9999  # Spark will listen on this port
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((TCP_IP, TCP_PORT))
server_socket.listen(1)
print(f"🚀 Waiting for Spark Streaming to connect on {TCP_IP}:{TCP_PORT}...")

conn, addr = server_socket.accept()
print(f"✅ Spark Streaming connected from {addr}")

# Callback function for processing messages
def callback(ch, method, properties, body):
    message = body.decode("utf-8")
    print(f"📩 Received from RabbitMQ: {message}")

    # Send the message to Spark Streaming via socket
    conn.sendall((message + "\n").encode("utf-8"))
    print(f"📤 Forwarded to Spark: {message}")

    # Acknowledge message processing
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming messages from RabbitMQ
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print("🔥 Waiting for messages from RabbitMQ. Press CTRL+C to exit.")
channel.start_consuming()

