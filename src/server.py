import copy
import zmq
import Queue
import sys
from hash_ring import HashRing
from random import randrange
import threading
import time

##########################################################################################################
# Data structure to store received and sending out messages
buffer1 = Queue.Queue()

buffer2 = Queue.Queue()
##########################################################################################################
# main funciton (part 1)
randnum = randrange(50, 1000)
# connext the socket
context = zmq.Context()

socket = context.socket(zmq.SUB)
socket.bind("tcp://*:" + str(int(sys.argv[2]) * 2 + 1))
# subscribe all incoming topic
socket.setsockopt(zmq.SUBSCRIBE, '')

# send them seperately
# only need one context
socket2 = context.socket(zmq.PUB)
socket2.bind("tcp://*:" + str(int(sys.argv[2]) * 2 + 2))

# argument: the machine number of all servers
addStr = []
for i in range(2, len(sys.argv)):
    srv_addr = sys.argv[i]
    addStr.append(srv_addr)

# first input should be a flag indicate whether the server instance will shut down after 20 sec.
flag = sys.argv[1]
cur = time.time()

##########################################################################################################
# Hash Ring


class myHashRing():
    def __init__(self, add):
        self.ring = HashRing(add)

    def getNode(self, team):
        return self.ring.get_node(team)

    def reHash(self, add):
        self.ring = HashRing(add)


ring = myHashRing(addStr)


##########################################################################################################
# class flag indicating whether this server is failed, if so, we want all three threads to be terminated.
class isFailed:
    def __init__(self):
        self.flag = False

    def getFlag(self):
        return self.flag

    def setFlag(self, Value):
        self.flag = Value


isEnd = isFailed()


##########################################################################################################
# Receiver


class Receiver(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while not isEnd.getFlag():
            string = socket.recv_string()
            print ("received message")
            if string.count(' ') != 0 and string.count(' ') != 1:
                # normal message
                team, pointScored, pointLost, strength, timePub = string.split()
                server = ring.getNode(team)
                if server == addStr[0]:
                    # message with topic we want
                    buffer1.put(string)
            elif string.count(' ') == 0:
                # server failed message
                for serveradd in addStr:
                    if serveradd == string:
                        buffer1.put(string)
                        print("server failed message received: " + string)
            else:
                print("publisher failed: " + string)
                buffer1.put(string)


##########################################################################################################
# data structures


# store 5 messages in a class


class History:
    def __init__(self, teamc, scored, lost, stren, tim, teamH, scoredH, lostH):
        self.team = int(teamc)
        self.pointScored = int(scored)
        self.pointLost = int(lost)
        self.strength = int(stren)
        self.timePub = float(tim)
        self.teamHis = teamH
        self.scoreHis = scoredH
        self.lostHis = lostH


#######################################################################################################################
# Processor


class Processor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.teamArrayHis = Queue.Queue()
        self.pointScoredArrayHis = Queue.Queue()
        self.pointLostArrayHis = Queue.Queue()
        self.teamArrayHisT = Queue.Queue()
        self.pointScoredArrayHisT = Queue.Queue()
        self.pointLostArrayHisT = Queue.Queue()

        # initialize history
        for x in range(0, 5):
            self.teamArrayHis.put(0)
            self.pointScoredArrayHis.put(0)
            self.pointLostArrayHis.put(0)

        # initialize new history
        self.teamNewHis = 0
        self.scoreNewHis = 0
        self.lostNewHis = 0

    # return all zipcode if it has same element
    def checksame(self, zlist):
        retlist = []
        for temp in zlist:
            # how many element have this value
            flag = zlist.count(temp)
            if flag > 1:
                if retlist.count(temp) == 0:
                    retlist.append(temp)
        return retlist

    # return the index of element who has the max strength
    def getmax(self, indexList, strengList):
        a = []
        for index in indexList:
            a.append(strengList[index])
        return a.index(max(a))

    def run(self):
        shutDownTime = 0
        while shutDownTime < 1000000000 and not isEnd.getFlag():
            i = 0
            # 5 History object
            hisList = []
            while i < 5:
                if buffer1.qsize() == 0:
                    continue
                # blocking is default
                string = buffer1.get()
                # if pubfailed
                if string.count(' ') == 1:
                    buffer2.put("%s" % (string))
                    continue
                # if other server failed, need remap
                if string.count(' ') == 0:
                    # need remap
                    if string in addStr:
                        print('removing ' + string + ' from list and rehashing')
                        addStr.remove(string)
                        ring.reHash(addStr)
                    continue
                # receive the message
                team, pointScored, pointLost, strength, timePub = string.split()

                server = ring.getNode(team)
                if server != addStr[0]:
                    continue
                else:
                    # store the data
                    # push in the new history
                    self.teamArrayHis.put(self.teamNewHis)
                    self.pointScoredArrayHis.put(self.scoreNewHis)
                    self.pointLostArrayHis.put(self.lostNewHis)

                    # pop up the old history
                    self.teamArrayHis.get()
                    self.pointScoredArrayHis.get()
                    self.pointLostArrayHis.get()

                    # next time this should be pushed in history queue
                    self.teamNewHis = int(team)
                    self.scoreNewHis = int(pointScored)
                    self.lostNewHis = int(pointLost)

                    # copy queue
                    self.teamArrayHisT.queue = copy.deepcopy(self.teamArrayHis.queue)
                    self.pointScoredArrayHisT.queue = copy.deepcopy(self.pointScoredArrayHis.queue)
                    self.pointLostArrayHisT.queue = copy.deepcopy(self.pointLostArrayHis.queue)

                    # History
                    # change to list and then use '/' as delimiter to make a string
                    tempt = [self.teamArrayHisT.get(), self.teamArrayHisT.get(), self.teamArrayHisT.get(),
                             self.teamArrayHisT.get(),
                             self.teamArrayHisT.get()]
                    teamHis = '/'.join(str(e) for e in tempt)
                    tempt = [self.pointScoredArrayHisT.get(), self.pointScoredArrayHisT.get(),
                             self.pointScoredArrayHisT.get(),
                             self.pointScoredArrayHisT.get(), self.pointScoredArrayHisT.get()]
                    scoreHis = '/'.join(str(e) for e in tempt)
                    tempt = [self.pointLostArrayHisT.get(), self.pointLostArrayHisT.get(),
                             self.pointLostArrayHisT.get(),
                             self.pointLostArrayHisT.get(), self.pointLostArrayHisT.get()]
                    lostHis = '/'.join(str(e) for e in tempt)

                    # store the message in class array
                    hisList.append(
                        History(team, pointScored, pointLost, int(strength), float(timePub), teamHis, scoreHis,
                                lostHis))

                    i += 1

            # get rid of the repeated topic hisList elements.
            # get all the zipcode
            zipList = [hisList[0].team, hisList[1].team, hisList[2].team, hisList[3].team,
                       hisList[4].team]

            # get all the strength
            strengList = [hisList[0].strength, hisList[1].strength, hisList[2].strength, hisList[3].strength,
                          hisList[4].strength]
            # if there is any element same
            # if not empty, that means there are same element
            retZipList = self.checksame(zipList)
            if retZipList:
                # for every repeated element
                for zip in retZipList:
                    indexList = []
                    i = 0
                    for ziporigin in zipList:
                        if zip == ziporigin:
                            indexList.append(i)
                        i += 1
                    # index of max strength element
                    maxindex = self.getmax(indexList, strengList)
                    # indexs of other repeated elemtns
                    indexList.pop(maxindex)
                    # pop up these repeated hisList elemtents
                    # sort the index reversely
                    sortedIndexList = sorted(indexList)
                    sortedIndexList.reverse()
                    for index in sortedIndexList:
                        hisList.pop(index)
                        zipList.pop(index)

            # send all history
            for his in hisList:
                # send last 5 infor (if repeated, not send)
                buffer2.put("%i %i %i %i %f %s %s %s" % (
                    his.team, his.pointScored, his.pointLost, his.strength, his.timePub, his.teamHis, his.scoreHis,
                    his.lostHis))


##########################################################################################################
# Sender


class Sender(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if buffer2.qsize() > 0:
                string = buffer2.get()
                if flag == '1' and time.time() - cur > 20:
                    # if failed, send out our own machine number
                    socket2.send_string(addStr[0])
                    isEnd.setFlag(True)
                else:
                    # which means publisher failed or normal message
                    socket2.send_string(string)


##########################################################################################################
# main function (part 2)


r = Receiver()
p = Processor()
s = Sender()

r.start()
p.start()
s.start()