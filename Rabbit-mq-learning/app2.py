from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, ValidationError, validator
import pika
import logging
from typing import Optional
import aiofiles

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Message(BaseModel):
    content: Optional[str] = None
    url: Optional[str] = None
    key_name: str

    @validator('content', 'url', pre=True, always=True)
    def validate_content_or_url(cls, v, values, **kwargs):
        if 'content' in values and values['content'] and 'url' in values and values['url']:
            raise ValueError('Only one of content or url should be provided')
        return v

# Create a global connection and channel to be used throughout the application
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue once on application startup
channel.queue_declare(queue='default_queue', durable=True, arguments={'x-queue-type': 'classic'})

def publish_to_rabbitmq(key_name: str, content: str):
    try:
        channel.queue_declare(queue=key_name, durable=True, arguments={'x-queue-type': 'classic'})
        channel.basic_publish(exchange='',
                              routing_key=key_name,
                              body=content,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        logger.info(f"Published message to queue {key_name}")
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
async def publish_message(
    key_name: str = Form(...),
    content: Optional[str] = Form(None),
    url: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None)
):
    if (content and url) or (content and file) or (url and file):
        raise HTTPException(status_code=400, detail="Only one of content, url, or file should be provided")
    if not content and not url and not file:
        raise HTTPException(status_code=400, detail="One of content, url, or file must be provided")

    message_content = None

    if content:
        message_content = content
    elif url:
        message_content = url
    elif file:
        async with aiofiles.open(file.filename, 'rb') as f:
            message_content = await f.read()

    publish_to_rabbitmq(key_name, message_content)
    return {"status": "Message published successfully"}
