from time import sleep
import zmq
import sys
from random import randrange
import time

randnum = randrange(50, 100)

context = zmq.Context()

socket = context.socket(zmq.PUB)

addStr = []
# second and more argument is server ip
for i in range(3, len(sys.argv)):
    srv_addr = sys.argv[i]
    addStr.append(srv_addr)

for addr in addStr:
    socket.connect("tcp://" + addr + ":5556")

# first argument is strength of pub, 0~...
strength = int(sys.argv[1]) if len(sys.argv) > 1 else 0

#second argument is zipcode
zipcode = int(sys.argv[2]) if len(sys.argv) > 1 else 37215
print("send zipcode %i" % zipcode)

ShutDownTime = 0
while ShutDownTime < 1000:

    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)
    pubTime=time.time()
    socket.send_string("%i %i %i %i %f" % (zipcode, temperature, relhumidity, strength, pubTime))

    # print "send messages"
    sleep(2)
    ShutDownTime+=1

    if ShutDownTime == randnum:
        failedstr = 'pubfailed'
        socket.send_string("%i %s" % (zipcode, failedstr))
        break
