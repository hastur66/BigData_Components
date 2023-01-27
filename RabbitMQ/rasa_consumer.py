import json
import pika

import os
from dotenv import load_dotenv

load_dotenv()

URL=os.getenv("HOST")
PORT=os.getenv("PORT")
USERNAME=os.getenv("USERNAME")
PASSWORD=os.getenv("PASSWORD")

def _callback(ch, method, properties, body):
        # Do something useful with your incoming message body here, e.g.
        # saving it to a database
        print("Received event {}".format(json.loads(body)))

if __name__ == "__main__":

    # RabbitMQ credentials with username and password
    #credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    params = pika.URLParameters(URL)

    # Pika connection to the RabbitMQ host - typically 'rabbit' in a
    # docker environment, or 'localhost' in a local environment
    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host=URL, port=PORT, credentials=credentials)
    # )
    connection = pika.BlockingConnection(params)

    # start consumption of channel
    channel = connection.channel()
    channel.basic_consume(queue="bot_events", on_message_callback=_callback, auto_ack=True)
    channel.start_consuming()