import copy
import zmq
import Queue
import sys
from hash_ring import HashRing
from random import randrange
import threading

##########################################################################################################
# Data structure to store received and sending out messages
buffer1 = Queue.Queue()

buffer2 = Queue.Queue()
##########################################################################################################
randnum = randrange(50, 1000)
# connext the socket
context = zmq.Context()

socket = context.socket(zmq.SUB)
socket.bind("tcp://*:" + str(int(sys.argv[1]) * 2 + 1))
# subscribe all incoming topic
socket.setsockopt(zmq.SUBSCRIBE, '')

# send them seperately
# only need one context
socket2 = context.socket(zmq.PUB)
socket2.bind("tcp://*:" + str(int(sys.argv[1]) * 2 + 2))

# argument: the ip of all server
# first is ip of this server
addStr = []
for i in range(1, len(sys.argv)):
    srv_addr = sys.argv[i]
    addStr.append(srv_addr)


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
            print("ready to receive")
            string = socket.recv_string()
            print ("received message")
            if string.count(' ') != 0 and string.count(' ') != 1:
                # normal message
                team, scoreget, scorelost, strength, timePub = string.split()
                server = ring.getNode(team)
                if server == addStr[0]:
                    # message with topic we want
                    buffer1.put(string)
            else:
                # failed message
                buffer1.put(string)


##########################################################################################################
# data structures


# store 5 messages in a class


class History:
    def __init__(self, zipc, tem, rel, stren, tim, zipH, temH, relH):
        self.zipcode = int(zipc)
        self.temperature = int(tem)
        self.relhumidity = int(rel)
        self.strength = int(stren)
        self.timePub = float(tim)
        self.zipHis = zipH
        self.temHis = temH
        self.relHis = relH


#######################################################################################################################
# Processor


class Processor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.zipcodeArrayHis = Queue.Queue()
        self.temperatureArrayHis = Queue.Queue()
        self.relhumidityArrayHis = Queue.Queue()
        self.zipcodeArrayHisT = Queue.Queue()
        self.temperatureArrayHisT = Queue.Queue()
        self.relhumidityArrayHisT = Queue.Queue()

        # initialize history
        for x in range(0, 5):
            self.zipcodeArrayHis.put(0)
            self.temperatureArrayHis.put(0)
            self.relhumidityArrayHis.put(0)

        # initialize new history
        self.zipNewHis = 0
        self.temNewHis = 0
        self.relNewHis = 0

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
                print ("ready to receive")
                # blocking is default
                string = socket.recv_string()
                print ("received message")
                # if pubfailed
                if string.count(' ') == 1:
                    buffer2.put("%s" % (string))
                    continue
                # if other server failed, need remap
                if string.count(' ') == 0:
                    # need remap
                    if string in addStr:
                        addStr.remove(string)
                        ring.reHash(addStr)
                    continue
                # receive the message
                zipcode, temperature, relhumidity, strength, timePub = string.split()
                #####################################################################################
                print(zipcode)
                #####################################################################################
                server = ring.getNode(zipcode)
                if server != addStr[0]:
                    continue
                else:
                    # store the data
                    # push in the new history
                    self.zipcodeArrayHis.put(self.zipNewHis)
                    self.temperatureArrayHis.put(self.temNewHis)
                    self.relhumidityArrayHis.put(self.relNewHis)

                    # pop up the old history
                    self.zipcodeArrayHis.get()
                    self.temperatureArrayHis.get()
                    self.relhumidityArrayHis.get()

                    # next time this should be pushed in history queue
                    self.zipNewHis = int(zipcode)
                    self.temNewHis = int(temperature)
                    self.relNewHis = int(relhumidity)

                    # copy queue
                    self.zipcodeArrayHisT.queue = copy.deepcopy(self.zipcodeArrayHis.queue)
                    self.temperatureArrayHisT.queue = copy.deepcopy(self.temperatureArrayHis.queue)
                    self.relhumidityArrayHisT.queue = copy.deepcopy(self.relhumidityArrayHis.queue)

                    # History
                    # change to list and then use '/' as delimiter to make a string
                    tempt = [self.zipcodeArrayHisT.get(), self.zipcodeArrayHisT.get(), self.zipcodeArrayHisT.get(),
                             self.zipcodeArrayHisT.get(),
                             self.zipcodeArrayHisT.get()]
                    zipHis = '/'.join(str(e) for e in tempt)
                    tempt = [self.temperatureArrayHisT.get(), self.temperatureArrayHisT.get(),
                             self.temperatureArrayHisT.get(),
                             self.temperatureArrayHisT.get(), self.temperatureArrayHisT.get()]
                    temHis = '/'.join(str(e) for e in tempt)
                    tempt = [self.relhumidityArrayHisT.get(), self.relhumidityArrayHisT.get(),
                             self.relhumidityArrayHisT.get(),
                             self.relhumidityArrayHisT.get(), self.relhumidityArrayHisT.get()]
                    relHis = '/'.join(str(e) for e in tempt)

                    # store the message in class array
                    hisList.append(
                        History(zipcode, temperature, relhumidity, int(strength), float(timePub), zipHis, temHis,
                                relHis))

                    i += 1

            # get rid of the repeated topic hisList elements.
            # get all the zipcode
            zipList = [hisList[0].zipcode, hisList[1].zipcode, hisList[2].zipcode, hisList[3].zipcode,
                       hisList[4].zipcode]

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
                    print(zipList)
                    for index in sortedIndexList:
                        hisList.pop(index)
                        zipList.pop(index)
                        print(zipList)

            # send all history
            for his in hisList:
                if shutDownTime == randnum:
                    failedServerStr = 'serverfailed'
                    buffer2.put("%s" % (sys.argv[1]))
                # send last 5 infor (if repeated, not send)
                buffer2.put("%i %i %i %i %f %s %s %s" % (
                    his.zipcode, his.temperature, his.relhumidity, his.strength, his.timePub, his.zipHis, his.temHis,
                    his.relHis))
                print ("send message")

            if shutDownTime == randnum:
                break


##########################################################################################################
# Sender


class Sender(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if buffer2.qsize() > 0:
                string = buffer2.get()
                if string.count(' ') == 0:
                    # which means server failed,
                    socket2.send_string(string)
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
