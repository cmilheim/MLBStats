import sys
import statsapi
import logging
from prettyformatter import pprint
import json
import pika

def main(debug):
    # Set up logger
    mainLogs = logging.getLogger(__name__)
    mainLogs.setLevel(logging.DEBUG)
    ch = logging.FileHandler('mainLogger.log', mode='w')
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    mainLogs.addHandler(ch)
    
    try:
        mainLogs.info("Retrieving teams")
        teams = statsapi.get('teams', {'sportId':1})['teams']
        mainLogs.debug('Teams: {}'.format(teams))
        mainLogs.debug('Teams retrieved: %s', len(teams))
    except Exception as e:
        mainLogs.error("Error retrieving teams: {}".format(e))
        sys.exit(1)

    for team in range(len(teams)):
        # Reorganize some keys
        teams[team]['teamId'] = teams[team]['id']
        teams[team]['venueId'] = teams[team]['venue']['id']
        teams[team]['venueName'] = teams[team]['venue']['name']
        teams[team]['leagueName'] = teams[team]['league']['name']
        teams[team]['divisionName'] = teams[team]['division']['name']

        # Remove old keys
        keys_to_remove = (
                'id', 'springLeague', 'link', 'springVenue', 'teamCode', 'fileCode', 'sport',
                'venue', 'league', 'division'
                )
        for key in keys_to_remove:
            teams[team].pop(key, None)

    if debug is True: pprint(teams)

    mainLogs.info("Building RabbitMQ Connection")
    try:
        creds = pika.PlainCredentials('cmilheim', 'rmqpassword')
        rmq = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=creds))
        channel = rmq.channel()
        channel.queue_declare(queue='teams')
    except Exception as e:
        mainLogs.error("Error building RabbitMQ connection: {}".format(e))
        sys.exit(1)

    mainLogs.info("RabbitMQ connection established")

    mainLogs.info("Attempt to send RabbitMQ message")
    channel.queue_declare(queue='teams')

    try:
        for team in range(len(teams)):
            channel.basic_publish(exchange='', routing_key='teams', body=json.dumps(teams[team]))
    except Exception as e:
        mainLogs.error("Error publishing to RabbitMQ: {}".format(e))
        sys.exit(1)

    mainLogs.info("Closing RabbitMQ connection")
    rmq.close()

    sys.exit(0)

if __name__ == "__main__":
    debug = False
    if len(sys.argv) > 1 and sys.argv[1].upper() == 'DEBUG':
        debug = True

    main(debug)