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
                teamLogs.info("Teams queue is empty. Exiting.")
                break
            dtag = method.delivery_tag
            teamLogs.debug("method - {}".format(method))
            teamLogs.debug("dtag - {}".format(dtag))
            teamLogs.debug("header - {}".format(header))
            teamLogs.debug("body - {}".format(body))
            teamLogs.debug("body decoded - {}".format(body.decode("utf-8")))
        except Exception as e:
            teamLogs.error("Error consuming from RabbitMQ: {}".format(e))
            sys.exit(1)

        # Perform upsert on MongoDB Teams collection
        teamLogs.info("RabbitMQ message received.  Updating MongoDB")
        try:            
            database = client["MLBStats"]
            collection = database["Teams"]
            teamsDict = json.loads(body)

            for k,v in teamsDict.items():
                teamLogs.debug("k:v - {}:{}".format(str(k),v))
                collection.find_one_and_update(
                    filter={'teamId': teamsDict['teamId']}, 
                    update={'$set': {k: v}}, 
                    upsert=True
                )

            collection.find_one_and_update(
                filter={'teamId': teamsDict['teamId']}, 
                update={'$set': {'lastUpdate': datetime.now()}}, 
                upsert=True
            )
            
            teamLogs.info("Mongo upsert completed")
        except Exception as e:
            teamLogs.error("Error connecting to Mongo: {}".format(e))
            sys.exit(1)

        # Acknowledge RabbitMQ Teams message
        teamLogs.info("Acknowledging RabbitMQ message")
        try:
            if channel.is_open:
                channel.basic_ack(dtag)
        except Exception as e:
            teamLogs.error("Error acknowleding RabbitMQ message")
            sys.exit(1)

        # Send Roster job to RabbitMQ
        teamLogs.info("Sending teamId to roster queue")
        try:
            channel.queue_declare(queue='roster')
            job_body = {"teamId": teamsDict['teamId']}
            channel.basic_publish(exchange='', routing_key='roster', body=json.dumps(job_body))
        except Exception as e:
            teamLogs.error("Error sending teamId to roster queue: {}".format(e))
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