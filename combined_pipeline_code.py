import os
import socket
import asyncio
import random
import sys
import signal
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from datetime import datetime
import pandas as pd 
import socket
from datetime import datetime
import time
from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
conn_str = "HostName=AbbaasHub.azure-devices.net;DeviceId=pi;SharedAccessKey=C3aiRrwSPiShwe5GuSoZidQ8K+D+mRmxmoMG0PhlrlI=;GatewayHostName=mcpslab"
ca_cert = "azure-iot-test-only.root.ca.cert.pem"
certfile = open(ca_cert)
root_ca_cert = certfile.read()

async def test_pub():
    client_gsu = MQTTClient()
    await client_gsu.connect('mqtt://localhost:1883')

# The device connection authenticates your device to your IoT hub. The connection string for 
# a device should never be stored in code. For the sake of simplicity we're using an environment 
# variable here. If you created the environment variable with the IDE running, stop and restart 
# the IDE to pick up the environment variable.
#
# You can use the Azure CLI to find the connection string:
#     az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyNodeDevice --output table

client = IoTHubDeviceClient.create_from_connection_string(
        connection_string=conn_str, server_verification_cert=root_ca_cert)

# Define the JSON message to send to IoT Hub.
TEMPERATURE = 20.0
HUMIDITY = 60
RADON=100.0
PRESSURE=100.0
MOISTURE = 30.0
VOC=5.0
CO2 = 10.0
O2 = 20.0
GAS = 10.0
MSG_TXT = '{{"deviceid": "{deviceid}","datetime": "{datetimenow}","messageid": {id1},"temperature": {temperature},"humidity": {humidity},"pressure":{pressure},"moisture":{moisture},"VOC":{voc},"CO2":{co2},"O2":{o2},"gas":{gas},"radon":{radon}}}'

def keyboard_interrupt_handler(signal, frame):
	print ( "IoTHubClient sample stopped" )
	sys.exit(0)

async def main():
	print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
	id=0
	#deviceid = pwd.getpwuid(os.getuid())[0]
	deviceid = socket.gethostname()
	#deviceid = 'pi'
	# Process the keyboard interrupt.
	signal.signal(signal.SIGINT, keyboard_interrupt_handler)    
	while True:
		# Build the message with simulated telemetry values.
		try:
			datetimenow = str(datetime.now().isoformat())
			temperature = TEMPERATURE + (random.random() * 15)
			humidity = HUMIDITY + (random.random() * 20)
			radon = RADON + (random.random() * 10)
			pressure = PRESSURE + (random.random() * 10)
			moisture = MOISTURE + (random.random() * 15)
			voc = VOC + (random.random() * 10)
			co2 = CO2 + (random.random() * 10)
			o2 = O2 + (random.random() * 10)
			gas = GAS + (random.random() * 10)
			msg_txt_formatted = MSG_TXT.format(deviceid = deviceid,datetimenow=datetimenow,id1=id,temperature=temperature, humidity=humidity, radon=radon, pressure=pressure, moisture=moisture, voc = voc, co2 = co2, o2 =o2, gas=gas)
			message = Message(msg_txt_formatted)
            message_gsu = {'deviceid':devid, 'messageid':messageid, 'datetime':datetimenow, 'temperature':temperature, 'humidity':humidity, 'pressure': pressure, 'moisture':moisture,
'VOC':VOC, 'CO2':CO2, 'O2':O2, 'gas':gas, 'radon':radon}
            MQTT_MSG = json.dumps(message_gsu).encode('utf-8')
		    await client.publish("abbaas/test", MQTT_MSG,qos=QOS_1)await client.publish("abbaas/test", MQTT_MSG,qos=QOS_1)
			header = "deviceid, datetime, messageid, temperature, humidity, pressure, moisture, VOC, CO2, O2, gas,radon\n"
			if not os.path.exists('log.csv'):
				with open('log.csv','w+') as f:
					f.write(header)
			with open('log.csv','a+') as f:	
				f.write(f'{deviceid},{datetimenow},{id},{temperature},{humidity},{pressure},{moisture},{voc},{co2},{o2},{gas},{radon}\n')
			await client.send_message(message)
			print( "Sending message: {}".format(message) )
			print ( "Message successfully sent" )
			id+=1
			await asyncio.sleep(5)
		except:
			datetimenow = str(datetime.now().isoformat())
			temperature = TEMPERATURE + (random.random() * 15)
			humidity = HUMIDITY + (random.random() * 20)
			radon = RADON + (random.random() * 10)
			pressure = PRESSURE + (random.random() * 10)
			moisture = MOISTURE + (random.random() * 15)
			voc = VOC + (random.random() * 10)
			co2 = CO2 + (random.random() * 10)
			o2 = O2 + (random.random() * 10)
			gas = GAS + (random.random() * 10)
			msg_txt_formatted = MSG_TXT.format(deviceid = deviceid,datetimenow=datetimenow,id1=id,temperature=temperature, humidity=humidity, radon=radon, pressure=pressure, moisture=moisture, voc = voc, co2 = co2, o2 =o2, gas=gas)
			message = Message(msg_txt_formatted)
			header = "deviceid, datetime, messageid, temperature, humidity, pressure, moisture, VOC, CO2, O2, gas,radon\n"
			if not os.path.exists('log.csv'):
				with open('log.csv','w+') as f:
					f.write(header)
			with open('log.csv','a+') as f:	
				f.write(f'{deviceid},{datetimenow},{id},{temperature},{humidity},{pressure},{moisture},{voc},{co2},{o2},{gas},{radon}\n')
			print("Problem with connection saving in log.csv")
			print("Saving Message: {}".format(message))
			print("Message saved successfully")
			id+=1
			await asyncio.sleep(5)
    await client_gsu.disconnect()
	await client.shutdown()

if __name__ == '__main__':
	print ( "IoT Hub Quickstart #1 - Simulated device" )
	print ( "Press Ctrl-C to exit" )
	asyncio.run(main())
