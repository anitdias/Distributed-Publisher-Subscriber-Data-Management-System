import json
import threading
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, Column, Float, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists, create_database
from scripts.logging.logger import logger  # Ensure you have a proper logger setup
from queue import Queue
import yaml

# Load configuration
with open('/subscriber/config.yml', 'r') as file:
    config_data = yaml.safe_load(file)

DATABASE_USER = config_data['database']['DATABASE_USER']
DATABASE_PASSWORD = config_data['database']['DATABASE_PASSWORD']
DATABASE_HOST = config_data['database']['DATABASE_HOST']
DATABASE_PORT = config_data['database']['DATABASE_PORT']
DATABASE_NAME = config_data['database']['DATABASE_NAME']

Base = declarative_base()

# Define models
class Delhi(Base):
    __tablename__ = 'delhi'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String)
    temperature = Column(Float)
    timestamp = Column(String)

class Mumbai(Base):
    __tablename__ = 'mumbai'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String)
    temperature = Column(Float)
    timestamp = Column(String)

class Bangalore(Base):
    __tablename__ = 'bangalore'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String)
    temperature = Column(Float)
    timestamp = Column(String)

class Chennai(Base):
    __tablename__ = 'chennai'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String)
    temperature = Column(Float)
    timestamp = Column(String)

class Kochi(Base):
    __tablename__ = 'kochi'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String)
    temperature = Column(Float)
    timestamp = Column(String)

class Database:

    DATABASE_URL = f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}'

    def __init__(self):
        self.engine = create_engine(self.DATABASE_URL)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            # logger.info(f"Created database at {self.DATABASE_URL}")
        Base.metadata.create_all(self.engine)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.queues = {city: Queue() for city in ["delhi", "mumbai", "bangalore", "chennai", "kochi"]}
        self.threads = {city: threading.Thread(target=self.process_queue, args=(city,)) for city in
                        ["delhi", "mumbai", "bangalore", "chennai", "kochi"]}
        self.message_count = {city: {'received': 0, 'processed': 0} for city in
                              ["delhi", "mumbai", "bangalore", "chennai", "kochi"]}
        for thread in self.threads.values():
            thread.start()
        # logger.info("Database and threading setup completed.")

    def insert_data(self, data):
        city = data.get('city')
        if city not in self.queues:
            logger.error(f"Invalid city in data: {city}")
            return
        self.message_count[city]['received'] += 1
        self.queues[city].put(data)
        # logger.info(f"Data enqueued for city '{city}': {data}")

    def process_queue(self, city):
        while True:
            data = self.queues[city].get()
            if data is None:
                break
            self.insert_data_to_table(city, data)
            self.message_count[city]['processed'] += 1
            # logger.info(f"Data processed from queue for city '{city}'.")

    def insert_data_to_table(self, city, data):
        session = self.SessionFactory()
        try:
            model_class = globals()[city.capitalize()]
            new_entry = model_class(
                city=data['city'],
                temperature=data['temperature'],
                timestamp=data['timestamp']
            )
            session.add(new_entry)
            session.commit()
            logger.info(f"Data inserted into {model_class.__tablename__}: {data}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to insert data into {model_class.__tablename__}: {e}")
            session.rollback()
        finally:
            session.close()

    def shutdown(self):
        logger.info("Shutting down database processing.")
        for city in self.queues:
            self.queues[city].put(None)
        for thread in self.threads.values():
            thread.join()
        logger.info("All threads have finished execution.")

    def check_all_messages_processed(self):
        all_processed = True
        for city, counts in self.message_count.items():
            if counts['received'] != counts['processed']:
                logger.warning(f"City {city} - Received: {counts['received']}, Processed: {counts['processed']}")
                all_processed = False
        if all_processed:
            logger.info("All messages processed successfully.")
        return all_processed
