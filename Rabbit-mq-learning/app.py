# RabbitMQ publisher using FastAPI and Pika 
## This is simple fastapi that create new queue and publish message to that queue


from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Message(BaseModel):
    content: str
    key_name: str

# Create a global connection and channel to be used throughout the application
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue once on application startup
channel.queue_declare(queue='default_queue', durable=True, arguments={'x-queue-type': 'classic'})

def publish_to_rabbitmq(message: Message):
    try:
        channel.queue_declare(queue=message.key_name, durable=True, arguments={'x-queue-type': 'classic'})
        channel.basic_publish(exchange='',
                              routing_key=message.key_name,
                              body=message.content,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        logger.info(f"Published message to queue {message.key_name}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Connection error: {e}")
        raise HTTPException(status_code=500, detail="Connection error with RabbitMQ")
    except pika.exceptions.AMQPChannelError as e:
        logger.error(f"Channel error: {e}")
        raise HTTPException(status_code=500, detail="Channel error with RabbitMQ")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error occurred")

@app.post("/publish")
def publish_message(message: Message):
    publish_to_rabbitmq(message)
    return {"status": "Message published successfully"}