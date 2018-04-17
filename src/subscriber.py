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
team_filter = sys.argv[1] if len(sys.argv) > 1 else "10001"

socket.setsockopt(zmq.SUBSCRIBE, '')

server = ring.get_node(team_filter)

portNow = str(int(server) * 2 + 2)

socket.connect("tcp://" + "localhost:" + portNow)

while True:
    team = score = lost = ['', '', '', '', '']
    teamInt = temInt = lostInt = [0, 0, 0, 0, 0]

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
            solverSocket.connect("tcp://" + "localhost:" + str(int(addr) * 2 + 1))

        loop = 0
        while loop < 15:
            time.sleep(1)
            solverSocket.send_string("%s" % string)
            loop += 1
        print("server failed messaged sent")

        socket.disconnect("tcp://" + "localhost:" + portNow)
        server = ring.get_node(team_filter)
        portNow = str(int(server) * 2 + 2)
        socket.connect("tcp://" + "localhost:" + portNow)

        continue

    # pub failed
    if numBlank == 1:
        failedtopic, failedstring=string.split()
        if failedtopic==team_filter:
            print("pub failed")
            break

    if numBlank == 7:
        teamStr, pointScoredStr, pointLostStr, strengthStr, timeStr, teamHisStr, scoredHisStr, lostHisStr = string.split()
        if teamStr != team_filter:
            print ("%s is not I want" % teamStr)
            continue
        # receive history
        team[0], team[1], team[2], team[3], team[4] = teamHisStr.split("/")
        score[0], score[1], score[2], score[3], score[4] = scoredHisStr.split("/")
        lost[0], lost[1], lost[2], lost[3], lost[4] = lostHisStr.split("/")

        # turn the string to int
        for k in range(5):
            teamInt[k] = int(team[k])
            temInt[k] = int(score[k])
            lostInt[k] = int(lost[k])

        print("This is received history")
        a = 1
        for l in range(0, 5):
            print("%ith point scored is %i" % (a, temInt[l]))
            print("%ith point lost is %i" % (a, lostInt[l]))
            a += 1
        timeSub=time.time()
        timeFlo=float(timeStr)
        timeUse=float(timeSub - timeFlo)
        print('This is received message')
        print("Team: %s, pointScored: %s, pointLost: %s, Strength: %s timeusing: %f" % (
            teamStr, pointScoredStr, pointLostStr, strengthStr, timeUse))
