import json
import sys
import threading
import time

import yaml

from client import Client
from generate_temp import generate_temp
from scripts.logger import logger

client = Client()

# Load configuration
try:
    with open('/publisher/config.yml', 'r') as file:
        config_data = yaml.safe_load(file)
        topic = config_data['client']['topic']
        thread_count = int(config_data['thread']['thread_count'])
        sleep_time = config_data['time']['sleep_time']
except Exception as e:
    logger.error(f"Failed to load configuration: {e}")
    sys.exit(1)


def fetch_publish():
    try:
        data = generate_temp()
        payload = json.dumps(data)
        client.publish(topic, payload)
        logger.info(f"Published data: {payload}")
    except Exception as e:
        logger.error(f"Error in fetch_publish: {e}")


try:
    logger.info("Starting the publisher service.")
    while True:
        threads = []
        for _ in range(thread_count):
            thread = threading.Thread(target=fetch_publish)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        logger.info(f"Sleeping for {sleep_time} seconds.")
        time.sleep(sleep_time)
except KeyboardInterrupt:
    logger.info("Process interrupted by user.")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
finally:
    client.disconnect()
    logger.info("Client disconnected.")
