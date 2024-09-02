# teams.py

import sys
from pymongo import MongoClient
import logging
from prettyformatter import pprint
import json
import pika
from datetime import datetime
import statsapi

def main(debug):
    # Set up logger
    rosterLogs = logging.getLogger(__name__)
    rosterLogs.setLevel(logging.DEBUG)
    ch = logging.FileHandler('rosterLogger.log', mode='w')
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    rosterLogs.addHandler(ch)
    
    rosterLogs.info("Building RabbitMQ Connection")
    try:
        creds = pika.PlainCredentials('cmilheim', 'rmqpassword')
        rmq = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=creds))
        channel = rmq.channel()
        channel.queue_declare(queue='roster')
    except Exception as e:
        rosterLogs.error("Error building RabbitMQ connection: {}".format(e))
        sys.exit(1)

    rosterLogs.info("RabbitMQ connection established")

    rosterLogs.info("Connecting to MongoDB")
    try:
        client = MongoClient("mongodb://root:rootpassword@localhost:27017")
        rosterLogs.debug("Mongo Databases {}".format(client.list_database_names()))
        rosterLogs.debug(client)
    except Exception as e:
        rosterLogs.error("Error connecting to MongoDB: {}".format(e))
        sys.exit(1)

    rosterLogs.info("Successfully connected to MongoDB")

    while True:
        rosterLogs.info("Attempt to retrieve RabbitMQ message")
        channel.queue_declare(queue='roster')
        try:
            method, header, body = channel.basic_get(queue='roster')
            if method is None:
                rosterLogs.info("Roster queue is empty. Exiting.")
                break
            dtag = method.delivery_tag
            rosterLogs.debug("method - {}".format(method))
            rosterLogs.debug("dtag - {}".format(dtag))
            rosterLogs.debug("header - {}".format(header))
            rosterLogs.debug("body - {}".format(body))
            rosterLogs.debug("body decoded - {}".format(body.decode("utf-8")))
        except Exception as e:
            rosterLogs.error("Error consuming from RabbitMQ: {}".format(e))
            sys.exit(1)

        # Retrieve roster for teamId
        try:
            rosterLogs.info("Retrieving team rosters")
            rosterDict = json.loads(body)
            rosterLogs.debug("Team ID: {}".format(rosterDict['teamId']))
            roster = statsapi.get('team_roster', {'teamId': rosterDict['teamId']})['roster']
            pprint(roster)
            print(type(roster))
            print("Length of roster: {}".format(len(roster)))
        except Exception as e:
            rosterLogs.error("Error retrieving team roster: {}".format(e))
            sys.exit(1)

        # Transform retrieved fields
        for player in range(len(roster)):
            print(player)
            pprint(roster[player])
            roster[player]['playerId'] = roster[player]['person']['id']
            fullName = roster[player]['person']['fullName'].split()
            roster[player]['firstName'] = fullName[0]
            roster[player]['LastName'] = fullName[1]
            roster[player]['position'] = roster[player]['position']['abbreviation']

            # Remove uneeded fields
            keys_to_remove = (
                'person', 'status', 'parentTeamId'
            )
            for key in keys_to_remove:
                roster[player].pop(key, None)

            if debug: 
                pprint(roster[player])

            rosterLogs.info("Sending Player ID {} to RabbitMQ".format(roster[player]['playerId']))
            try:
                channel.queue_declare(queue='player')
                job_body = {"playerId": roster[player]['playerId']}
                channel.basic_publish(exchange='', routing_key='player', body=json.dumps(job_body))
                rosterLogs.info("Pushed Player ID {} to queue".format(roster[player]['playerId']))
            except Exception as e:
                rosterLogs.error("Error sending to Players queue: {}".format(e))
                sys.exit(1)

        rosterDict['roster'] = roster
        rosterLogs.info("RabbitMQ message received.  Updating MongoDB")
        try:            
            database = client["MLBStats"]
            collection = database["Roster"]

            for k,v in rosterDict.items():
                rosterLogs.debug("k:v - {}:{}".format(str(k),v))
                res = collection.find_one_and_update(
                    filter={'teamId': rosterDict['teamId']},
                    update={'$set': {k: v}},
                    upsert=True
                )

            res = collection.find_one_and_update(
                filter={'teamId': rosterDict['teamId']},
                update={'$set': {'lastUpdate': datetime.now()}},
                upsert=True
            )
            rosterLogs.info("Mongo Insert Returned: {}".format(res))
        except Exception as e:
            rosterLogs.error("Error connecting to Mongo: {}".format(e))
            sys.exit(1)

        rosterLogs.info("Acknowledging RabbitMQ message")
        try:
            channel.queue_declare(queue='roster')
            if channel.is_open:
                channel.basic_ack(dtag)
        except Exception as e:
            rosterLogs.error("Error acknowleding RabbitMQ message")
            sys.exit(1)

    rosterLogs.info("Closing RabbitMQ connection")
    rmq.close()
    rosterLogs.info("Closing MongoDB connection")
    client.close()

    sys.exit(0)

if __name__ == "__main__":
    debug = False
    if len(sys.argv) > 1 and sys.argv[1].upper() == 'DEBUG':
        debug = True

    main(debug)