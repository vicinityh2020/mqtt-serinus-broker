import paho.mqtt.client as mqtt

import json
from serinus_db import SerinusDB


SERVER = 'm23.cloudmqtt.com'
USERNAME = ''
PASSWORD = ''
PORT = 10804

TOPIC = '#'

# TODO: ENV VARIABLES for Credentials
# TODO: MQTT over TLS with Cert from mqtt cloud
# TODO: proper logs


def on_connect(c, userdata, flags, rc):
    print('on_connect')

    # TODO: more response codes

    if rc == 5:
        print('Error: Invalid username or password!')
        exit(1)

    if rc != 0:
        print('Warning: Connected with code other than 0, errors may occur...')

    c.subscribe(TOPIC)


def on_message(c, userdata, msg):

    body = msg.payload.decode('utf_8')

    if not body:
        print('Empty data returned')
        return

    body = json.loads(body)
    if 'dataarray' not in body:
        print('message object does not contain \'dataarray\' key.')
        return

    data = body['dataarray']
    if len(data) < 1:
        print('malformed message. List is empty.')
        return

    for o in data:
        if 'data' not in o:
            print('message object does not contain \'data\' key.')
            continue

        record_data = o['data']
        for record in record_data:
            mongo_client.save(record)


if __name__ == '__main__':
    mongo_client = SerinusDB()

    mqtt_client = mqtt.Client()

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.username_pw_set(USERNAME, PASSWORD)
    print("Username and password successfully set!")

    try:
        mqtt_client.connect(host=SERVER, port=PORT)
        print('connection executed')
    except:
        print("Couldn't connect to serverURL address.")

    mqtt_client.loop_forever()
