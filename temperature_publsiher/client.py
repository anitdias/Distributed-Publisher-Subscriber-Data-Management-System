import yaml
from paho.mqtt import client as mqtt
from scripts.logger import logger

# Load configuration
with open('/publisher/config.yml', 'r') as file:
    config_data = yaml.safe_load(file)
host = config_data['client']['host']
port = config_data['client']['port']
keepalive = config_data['client']['keepalive']


class Client:

    def __init__(self):
        self.client = mqtt.Client()
        try:
            self.client.connect(host, int(port), int(keepalive))
            self.client.on_publish = self.on_publish
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
            self.client.publish(topic, msg, qos=0)
            logger.info(f"Published message to topic '{topic}' with QoS 0.")
        except Exception as e:
            logger.error(f"Failed to publish message to topic '{topic}': {e}")

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker.")
        except Exception as e:
            logger.error(f"Error during disconnection: {e}")
