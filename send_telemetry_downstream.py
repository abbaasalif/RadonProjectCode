import os
import socket
import asyncio
import random
import sys
import signal
import serial
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from datetime import datetime
def keyboard_interrupt_handler(signal, frame):
	print ( "IoTHubClient sample stopped" )
	sys.exit(0)

async def main():
	print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
	signal.signal(signal.SIGINT, keyboard_interrupt_handler) 
	conn_str = "HostName=AbbaasHub.azure-devices.net;DeviceId=Stone_Mountain;SharedAccessKey=/7mZkaiAEHcT/AOsWfRwKASLD3LpxsH0Q+SuR4dKSPo="
	with open('/home/pi/RadonProjectCode/time.csv','r') as f:
		times = f.read().splitlines()
	client = IoTHubDeviceClient.create_from_connection_string(
        connection_string=conn_str)
	site = 'Stone Mountain'
	MSG_TXT = '{{"datetime": "{datetimenow}","deviceid":{deviceid},"site":"{site}","moisture":{moisture}}}'
	count = 0
	datetimenow = None
	with open('/home/pi/RadonProjectCode/time.csv','r') as f:
		times = f.read().splitlines()   
	array_moisture = []
	deviceid='Stone Mountain Radon 1'
	while True:
		# Build the message with simulated telemetry values.
		try:
			if os.path.exists('/home/pi/RadonProjectCode/error.csv'):
				with open('/home/pi/RadonProjectCode/error.csv', 'r') as f:
					lines = f.readlines()
					for line in lines[1:]:
						dataline = line.strip().split(',')
						datetimenow, deviceid,site, moisture = dataline
						msg_txt_formatted = MSG_TXT.format(datetimenow=datetimenow, deviceid=deviceid ,site=site, moisture=moisture)
						message = Message(msg_txt_formatted)
						await client.send_message(message)
						print( "Sending message: {}".format(message) )
						print ( "Message successfully sent" )
			
			ser = serial.Serial("/dev/ttyACM0",115200)
			temp = ser.readline().decode('Ascii').strip()
			temp = int(temp)
			array_moisture.append(temp)
			datetimenow = str(datetime.now())
			header = "datetime, deviceid, site, moisture"
			if not os.path.exists('/home/pi/RadonProjectCode/log.csv'):
				with open('/home/pi/RadonProjectCode/log.csv','w+') as f:
					f.write(header + '\n')
			with open('/home/pi/RadonProjectCode/log.csv','a+') as f:	
				f.write(f'{datetimenow},{deviceid},{site},{temp}\n')
			print(f"datimetime: {datetimenow}, deviceid: {deviceid},Site: {site}, moisture: {temp}")
			if datetime.now().strftime("%H:%M") in times:  
				if count == 11:
					datetimenow = str(datetime.now())
					print(len(array_moisture))
					average = sum(array_moisture)/len(array_moisture)
					print(average)
					array_moisture.clear()
					print('datetime:',datetimenow,'hour average:',average)
					msg_txt_formatted = MSG_TXT.format(datetimenow=datetimenow, deviceid=deviceid,site=site, moisture=average)
					message = Message(msg_txt_formatted)
					await client.send_message(message)
					print( "Sending message: {}".format(message) )
					print ( "Message successfully sent" )
					count=0
				count += 1
		except:
			ser = serial.Serial("/dev/ttyACM0",115200)
			temp = ser.readline().decode('Ascii').strip()
			temp = int(temp)
			array_moisture.append(temp)
			datetimenow = str(datetime.now())
			header = "datetime, deivceid,site, moisture"
			if not os.path.exists('/home/pi/RadonProjectCode/log.csv'):
				with open('/home/pi/RadonProjectCode/log.csv','w+') as f:
					f.write(header+'\n')
			with open('/home/pi/RadonProjectCode/log.csv','a+') as f:	
				f.write(f'{datetimenow},{deviceid},{site},{temp}\n')
			if datetime.now().strftime("%H:%M") in times:  
				if count == 11:
					datetimenow = str(datetime.now())
					average = sum(array_moisture)/len(array_moisture)
					array_moisture.clear()
					if not os.path.exists('/home/pi/RadonProjectCode/error.csv'):
						with open('/home/pi/RadonProjectCode/error.csv','w+') as f:
							f.write(header+'\n')
					with open('/home/pi/RadonProjectCode/error.csv','a+') as f:	
						f.write(f'{datetimenow},{deviceid},{site},{temp}\n')
					count=0
				count += 1
	await client.shutdown()

if __name__ == '__main__':
	print ( "IoT Hub Quickstart #1 - Simulated device" )
	print ( "Press Ctrl-C to exit" )
	asyncio.run(main())
