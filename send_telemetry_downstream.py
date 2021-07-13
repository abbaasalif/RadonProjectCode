import os
import asyncio
import random
import sys
import signal
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message

conn_str = "HostName=AbbaasHub.azure-devices.net;DeviceId=pi;SharedAccessKey=C3aiRrwSPiShwe5GuSoZidQ8K+D+mRmxmoMG0PhlrlI=;GatewayHostName=cloud-vm"
ca_cert = "azure-iot-test-only.root.ca.cert.pem"
certfile = open(ca_cert)
root_ca_cert = certfile.read()

    # The client object is used to interact with your Azure IoT Edge device.
MSG_TXT = '{{"messageid": {id1},"temperature": {temperature},"humidity": {humidity}}}'



# The device connection authenticates your device to your IoT hub. The connection string for 
# a device should never be stored in code. For the sake of simplicity we're using an environment 
# variable here. If you created the environment variable with the IDE running, stop and restart 
# the IDE to pick up the environment variable.
#
# You can use the Azure CLI to find the connection string:
#     az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyNodeDevice --output table

client = IoTHubDeviceClient.create_from_connection_string(
        connection_string=conn_str, server_verification_cert=root_ca_cert
    )

# Define the JSON message to send to IoT Hub.
TEMPERATURE = 20.0
HUMIDITY = 60
MSG_TXT = '{{"messageid": {id1},"temperature": {temperature},"humidity": {humidity}}}'

def keyboard_interrupt_handler(signal, frame):
    print ( "IoTHubClient sample stopped" )
    sys.exit(0)

async def main():

    
    print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
    id=0
    # Process the keyboard interrupt.
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)    

    while True:

        # Build the message with simulated telemetry values.
        temperature = TEMPERATURE + (random.random() * 15)
        humidity = HUMIDITY + (random.random() * 20)
        msg_txt_formatted = MSG_TXT.format(id1=id,temperature=temperature, humidity=humidity)
        message = Message(msg_txt_formatted)

        # Add a custom application property to the message.
        # An IoT hub can filter on these properties without access to the message body.
        if temperature > 30:
          message.custom_properties["temperatureAlert"] = "true"
        else:
          message.custom_properties["temperatureAlert"] = "false"

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