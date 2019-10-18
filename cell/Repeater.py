from socket import *
from config import ConfigFile
import sys
import os

cfg = ConfigFile(os.environ['DFARM_CONFIG'])

rcv_port = cfg.getValue('cell','*','repeater_port')
if rcv_port == None:
        print('No repeater port defined')
        sys.exit(1)

rcv_host = cfg.getValue('cell','*','repeater_host')
if rcv_port == None:
        print('No repeater host defined')
        sys.exit(1)

bcast_addr = cfg.getValue('cell','*','broadcast')
if bcast_addr == None:
        print('No broadcast address defined')
        sys.exit(1)

bcast_port = cfg.getValue('cell','*','listen_port')
if bcast_port == None:
        print('No cell manager listener port defined')
        sys.exit(1)

bcast = (bcast_addr, bcast_port)

s1 = socket(AF_INET, SOCK_DGRAM)
s1.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
s2 = socket(AF_INET, SOCK_DGRAM)
s2.bind((rcv_host, rcv_port))
while 1:
        data, sndr = s2.rcvfrom(10000)
        try:
                s1.sendto(data, bcast)
        except:
                pass
