 # Code generated via "Slingshot" 
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
import asyncio
import json
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventHubSender:
    def __init__(self, connection_string, eventhub_name):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.producer = None

    async def initialize(self):
        try:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
        except Exception as e:
            logger.error(f"Failed to initialize EventHub producer: {str(e)}")
            raise

    async def send_messages(self, messages, batch_size=100):
        if not self.producer:
            await self.initialize()

        try:
            async with self.producer:
                # Create a batch
                event_data_batch = await self.producer.create_batch()

                for message in messages:
                    try:
                        # Add message to batch
                        event_data = EventData(json.dumps({
                            'message': message,
                            'timestamp': datetime.utcnow().isoformat()
                        }))
                        event_data_batch.add(event_data)

                        # If batch is full, send it
                        if len(event_data_batch) >= batch_size:
                            await self.producer.send_batch(event_data_batch)
                            event_data_batch = await self.producer.create_batch()

                    except ValueError as ve:
                        logger.error(f"Message too large for batch: {str(ve)}")
                        continue

                # Send any remaining messages
                if len(event_data_batch) > 0:
                    await self.producer.send_batch(event_data_batch)

        except EventHubError as eh_err:
            logger.error(f"EventHub error occurred: {str(eh_err)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error occurred: {str(e)}")
            raise

async def main():
    # Get connection details from environment variables
    connection_string = os.getenv('EVENTHUB_CONNECTION_STRING')
    eventhub_name = os.getenv('EVENTHUB_NAME')

    if not connection_string or not eventhub_name:
        raise ValueError("Please set EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME environment variables")

    # Sample messages
    sample_messages = [
        f"Test message {i}" for i in range(1, 11)
    ]

    sender = EventHubSender(connection_string, eventhub_name)
    await sender.send_messages(sample_messages)

if __name__ == "__main__":
    asyncio.run(main())