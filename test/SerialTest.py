#!/usr/bin/env python3
from pymodbus.client import ModbusSerialClient
import time

#init modbus device
client = ModbusSerialClient(method='rtu', port='/dev/ttyACM0', baudrate=9600)
client2 = ModbusSerialClient(method='rtu', port='/dev/ttyUSB0', baudrate=9600)
loop = False
relayState = False
gasState = True

def testBatch():
    #writeRly = client.write_coil(0, relayState, slave = 1)
    #writeRly = client.write_coil(1, relayState, slave = 1)
    #writeRly = client.write_coil(2, relayState, slave = 1)
    #writeRly = client.write_coil(3, relayState, slave = 1)
    writeRly = client.write_coil(4, relayState, slave = 1)
    #writeRly = client.write_coil(5, relayState, slave = 1)
    print(writeRly)
    time.sleep(0.2)
    getElect = client.read_holding_registers(0, 29, slave = 2)
    print(getElect)
    time.sleep(0.2)
    getTemp = client.read_holding_registers(4, 3, slave = 3)   
    print(getTemp.registers)
    time.sleep(0.2)
    if gasState:
        getH2 = client.read_holding_registers(0, 10, slave = 4)
        print(getH2.registers)
        time.sleep(0.2)
        getMoist = client.read_input_registers(0, 3, slave = 5)
        print(getMoist.registers)
        time.sleep(0.2)
    
    print("~~~")

#Loop
if loop:
    while True:
        testBatch()
        time.sleep(2)
else:   
    testBatch()
