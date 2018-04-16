import zmq
import sys
from hash_ring import HashRing
import time

context = zmq.Context()
socket = context.socket(zmq.SUB)

# argument: the ip of all server
addStr = []
for i in range(2, len(sys.argv)):
    srv_addr = sys.argv[i]
    addStr.append(srv_addr)

# hashring
ring = HashRing(addStr)

# filter
zip_filter = sys.argv[1] if len(sys.argv) > 1 else "10001"
# if isinstance(zip_filter, bytes):
#     zip_filter = zip_filter.decode('ascii')
socket.setsockopt(zmq.SUBSCRIBE, '')

server = ring.get_node(zip_filter)

socket.connect("tcp://" + "localhost:" + str(int(server)*2 + 2))

while True:
    zip = tem = rel = ['', '', '', '', '']
    zipInt = temInt = relInt = [0, 0, 0, 0, 0]

    i = 0
    print("ready to receive")
    string = socket.recv()
    print("message received")
    numBlank=string.count(' ')
    # server failed
    if numBlank== 0:
        # string is ip of failed server
        # remap
        addStr.remove(string)
        ring = HashRing(addStr)

        print("server failed, other servers need remap")
        solverSocket = context.socket(zmq.PUB)
        # connect all other server
        for addr in addStr:
            solverSocket.connect("tcp://" + "localhost:" + str(int(addr)*2 + 1))
        # other server need remap
        socket.send_string("%s" % string)
        continue

    # pub failed
    if numBlank == 1:
        failedtopic, failedstring=string.split()
        if failedtopic==zip_filter:
            print("pub failed")
            break

    if numBlank == 7:
        zipcodeStr, temperatureStr, relhumidityStr, strengthStr, timeStr, zipHisStr, temHisStr, relHisStr = string.split()
        if zipcodeStr != zip_filter:
            print ("%s is not I want" % zipcodeStr)
            continue
        # receive history
        zip[0], zip[1], zip[2], zip[3], zip[4] = zipHisStr.split("/")
        tem[0], tem[1], tem[2], tem[3], tem[4] = temHisStr.split("/")
        rel[0], rel[1], rel[2], rel[3], rel[4] = relHisStr.split("/")

        # turn the string to int
        for k in range(5):
            zipInt[k] = int(zip[k])
            temInt[k] = int(tem[k])
            relInt[k] = int(rel[k])

        print("This is received history")
        a = 1
        for l in range(0, 5):
            # if zipInt[l] == int(zip_filter):
            print("%ith temperature is %i" % (a, temInt[l]))
            print("%ith relhumidity is %i" % (a, relInt[l]))
            a += 1
        timeSub=time.time()
        timeFlo=float(timeStr)
        timeUse=float(timeSub - timeFlo)
        print('This is received message')
        print("Topic: %s, Temperature: %s, Humidity: %s, Strength: %s timeusing: %f" % (
            zipcodeStr, temperatureStr, relhumidityStr, strengthStr, timeUse))
