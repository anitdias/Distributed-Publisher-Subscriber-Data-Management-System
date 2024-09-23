import json
from paho.mqtt import client as mqtt
from postgres import Database
import yaml
from datetime import datetime, timedelta
from scripts.logging.logger import logger

# Load configuration
with open('/subscriber/config.yml', 'r') as file:
    config_data = yaml.safe_load(file)

topic = config_data['client']['topic']
host = config_data['client']['host']
port = config_data['client']['port']
keepalive = config_data['client']['keepalive']
omit_time = int(config_data['omit']['omit_time'])
date_format = '%Y-%m-%d %H:%M:%S'


class Client:
    def __init__(self, database):
        self.client = mqtt.Client()
        self.database = database
        try:
            self.client.connect(host, int(port), int(keepalive))
            self.client.on_publish = self.on_publish
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_subscribe = self.on_subscribe
            self.client.loop_start()
            logger.info(f"Connected to MQTT broker at {host}:{port} with keepalive {keepalive}.")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")

    def on_publish(self, client, userdata, result):
        if result == mqtt.MQTT_ERR_SUCCESS:
            logger.info("Message successfully published.")
        else:
            logger.error(f"Message publish failed with result code: {result}")

    def publish(self, topic, msg):
        try:
            self.client.publish(topic, msg)
            logger.info(f"Published message to topic '{topic}'.")
        except Exception as e:
            logger.error(f"Failed to publish message to topic '{topic}': {e}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker with result code {rc}. Subscribing to topic '{topic}'.")
            self.client.subscribe(topic, qos=0)
        else:
            logger.error(f"Failed to connect to MQTT broker, result code: {rc}")

    def handle_message(self, msg_payload):
        try:
            data = json.loads(msg_payload.decode())
            time_str = data.get('timestamp')
            given_datetime = datetime.strptime(time_str, date_format)
            minutes = timedelta(minutes=omit_time)
            if datetime.now() - given_datetime < minutes:
                self.database.insert_data(data)
                logger.info(f"Data inserted into database: {data}")
            else:
                logger.error(f"Data not inserted, timestamp crosses omit time")


        except Exception as e:
            logger.error(f"Failed to decode JSON: {e}")

    def on_message(self, client, userdata, msg):
        logger.info(f"Message received: {msg.payload.decode()}")
        self.handle_message(msg.payload)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        logger.info(f"Subscribed with mid: {mid}, granted QoS: {granted_qos}")

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker.")
        except Exception as e:
            logger.error(f"Error during disconnection: {e}")
        finally:
            self.database.shutdown()
            logger.info("Database shutdown.")
            self.database.check_all_messages_processed()
            logger.info("Checked all messages processed.")


if __name__ == "__main__":
    try:
        database = Database()
        mqtt_client = Client(database)
        logger.info("MQTT client started. Press Ctrl+C to exit.")
        while True:
            pass
    except KeyboardInterrupt:
        logger.info("Exiting...")
        mqtt_client.disconnect()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
