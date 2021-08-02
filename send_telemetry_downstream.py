import os
import pwd
import asyncio
import random
import sys
import signal
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from datetime import datetime

conn_str = "HostName=AbbaasHub.azure-devices.net;DeviceId=pi;SharedAccessKey=C3aiRrwSPiShwe5GuSoZidQ8K+D+mRmxmoMG0PhlrlI=;GatewayHostName=cloud-vm"
ca_cert = "/home/pi/Documents/azure iot/azure-iot-test-only.root.ca.cert.pem"
certfile = open(ca_cert)
root_ca_cert = certfile.read()


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
MSG_TXT = '{{"deviceid": "{deviceid}","datetime": "{datetimenow}","messageid": {id1},"temperature": {temperature},"humidity": {humidity},"radon":{radon}}}'

def keyboard_interrupt_handler(signal, frame):
	print ( "IoTHubClient sample stopped" )
	sys.exit(0)

async def main():
	print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
	id=0
	deviceid = pwd.getpwuid(os.getuid())[0]
	# Process the keyboard interrupt.
	signal.signal(signal.SIGINT, keyboard_interrupt_handler)    
	while True:
		# Build the message with simulated telemetry values.
		datetimenow = str(datetime.now().isoformat())
		temperature = TEMPERATURE + (random.random() * 15)
		humidity = HUMIDITY + (random.random() * 20)
		radon = RADON + (random.random() * 10)
		msg_txt_formatted = MSG_TXT.format(deviceid = deviceid,datetimenow=datetimenow,id1=id,temperature=temperature, humidity=humidity, radon=radon)
		message = Message(msg_txt_formatted)
		# Send the message.
		print( "Sending message: {}".format(message) )
		await client.send_message(message)
		print ( "Message successfully sent" )
		id+=1
		await asyncio.sleep(5)
	await client.shutdown()

if __name__ == '__main__':
	print ( "IoT Hub Quickstart #1 - Simulated device" )
	print ( "Press Ctrl-C to exit" )
	asyncio.run(main())