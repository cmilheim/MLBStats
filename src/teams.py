# teams.py

import sys
from pymongo import MongoClient
import logging
from prettyformatter import pprint
import json
import pika
from datetime import datetime

def main(debug):
    # Set up logger
    teamLogs = logging.getLogger(__name__)
    teamLogs.setLevel(logging.DEBUG)
    ch = logging.FileHandler('teamsLogger.log', mode='w')
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    teamLogs.addHandler(ch)
    
    teamLogs.info("Building RabbitMQ Connection")
    try:
        creds = pika.PlainCredentials('cmilheim', 'rmqpassword')
        rmq = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=creds))
        channel = rmq.channel()
        channel.queue_declare(queue='teams')
    except Exception as e:
        teamLogs.error("Error building RabbitMQ connection: {}".format(e))
        sys.exit(1)

    teamLogs.info("RabbitMQ connection established")

    teamLogs.info("Connecting to MongoDB")
    try:
        client = MongoClient("mongodb://root:rootpassword@localhost:27017")
        teamLogs.debug("Mongo Databases {}".format(client.list_database_names()))
        teamLogs.debug(client)
    except Exception as e:
        teamLogs.error("Error connecting to MongoDB: {}".format(e))
        sys.exit(1)

    teamLogs.info("Successfully connected to MongoDB")

    while True:
        teamLogs.info("Attempt to retrieve RabbitMQ message")
        channel.queue_declare(queue='teams')
        try:
            method, header, body = channel.basic_get(queue='teams')
            if method is None:
                break
            dtag = method.delivery_tag
            teamLogs.debug("method - {}".format(method))
            teamLogs.debug("dtag - {}".format(dtag))
            teamLogs.debug("header - {}".format(header))
            teamLogs.debug("body - {}".format(body))
            teamLogs.debug("body decoded - {}".format(body.decode("utf-8")))
            if body is None:
                teamLogs.info("Teams queue is empty. Exiting.")
                break
        except Exception as e:
            teamLogs.error("Error consuming from RabbitMQ: {}".format(e))
            sys.exit(1)

        teamLogs.info("RabbitMQ message received.  Updating MongoDB")
        try:            
            database = client["MLBStats"]
            collection = database["Teams"]
            teamsDict = json.loads(body)

            for k,v in teamsDict.items():
                teamLogs.debug("k:v - {}:{}".format(str(k),v))
                res = collection.find_one_and_update(filter={'id': teamsDict['id']}, update={'$set': {k: v}}, upsert=True)

            res = collection.find_one_and_update(filter={'id': teamsDict['id']}, update={'$set': {'lastUpdate': datetime.now()}}, upsert=True)
            teamLogs.info("Mongo Insert Returned: {}".format(res))
        except Exception as e:
            teamLogs.error("Error connecting to Mongo: {}".format(e))
            sys.exit(1)

        teamLogs.info("Acknowledging RabbitMQ message")
        try:
            if channel.is_open:
                channel.basic_ack(dtag)
        except Exception as e:
            teamLogs.error("Error acknowleding RabbitMQ message")
            sys.exit(1)

    teamLogs.info("Closing RabbitMQ connection")
    rmq.close()
    teamLogs.info("Closing MongoDB connection")
    client.close()

    sys.exit(0)

if __name__ == "__main__":
    debug = False
    if len(sys.argv) > 1 and sys.argv[1].upper() == 'DEBUG':
        debug = True

    main(debug)