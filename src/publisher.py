from time import sleep
import zmq
import sys
from random import randrange
import time

context = zmq.Context()

socket = context.socket(zmq.PUB)

addStr = []
# second and more argument is server ip
for i in range(4, len(sys.argv)):
    srv_addr = sys.argv[i]
    addStr.append(srv_addr)

for addr in addStr:
    socket.connect("tcp://" + "localhost:" + str(int(addr)*2 + 1))

# first argument is strength of pub, 0~...
strength = int(sys.argv[1]) if len(sys.argv) > 1 else 0

#second argument is zipcode
teamid = int(sys.argv[2]) if len(sys.argv) > 1 else 1
print("send zipcode %i" % teamid)

#this flag is introduced to indicate whether this publisher will be failed down after 20 secs.
flag = sys.argv[3]
cur = time.time()

while True:

    pointScored = randrange(80, 150)
    pointLost = randrange(80, 140)
    pubTime=time.time()
    socket.send_string("%i %i %i %i %f" % (teamid, pointScored, pointLost, strength, pubTime))

    # print "send messages"
    sleep(2)

    if flag == '1' and time.time() - cur > 20:
        failedstr = 'pubfailed'
        socket.send_string("%i %s" % (teamid, failedstr))
        break
