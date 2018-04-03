import copy
import zmq
import Queue
import sys
from hash_ring import HashRing
from random import randrange

randnum = randrange(50, 1000)
# connext the socket
context = zmq.Context()

socket = context.socket(zmq.SUB)
socket.bind("tcp://*:5556")
# subscribe all incoming topic
socket.setsockopt(zmq.SUBSCRIBE, '')

# send them seperately
# only need one context
socket2 = context.socket(zmq.PUB)
socket2.bind("tcp://*:5550")

# argument: the ip of all server
# first is ip of this server
addStr = []
for i in range(1, len(sys.argv)):
    srv_addr = sys.argv[i]
    addStr.append(srv_addr)

# hashring
ring = HashRing(addStr)


##########################################################################################################
# function

# return all zipcode if it has same element
def checksame(zlist):
    retlist = []
    for temp in zlist:
        # how many element have this value
        flag = zipList.count(temp)
        if flag > 1:
            if retlist.count(temp) == 0:
                retlist.append(temp)
    return retlist


# return the index of element who has the max strength
def getmax(indexList, strengList):
    a = []
    for index in indexList:
        a.append(strengList[index])
    return a.index(max(a))


##########################################################################################################
# data structures

zipcodeArrayHis = Queue.Queue()
temperatureArrayHis = Queue.Queue()
relhumidityArrayHis = Queue.Queue()
zipcodeArrayHisT = Queue.Queue()
temperatureArrayHisT = Queue.Queue()
relhumidityArrayHisT = Queue.Queue()

# initialize history
for x in range(0, 5):
    zipcodeArrayHis.put(0)
    temperatureArrayHis.put(0)
    relhumidityArrayHis.put(0)

# initialize new history
zipNewHis = 0
temNewHis = 0
relNewHis = 0


# store 5 messages in a class
class History:
    def __init__(self, zipc, tem, rel, stren, tim, zipH, temH, relH):
        self.zipcode = int(zipc)
        self.temperature = int(tem)
        self.relhumidity = int(rel)
        self.strength = int(stren)
        self.timePub=float(tim)
        self.zipHis = zipH
        self.temHis = temH
        self.relHis = relH


##########################################################################################################
shutDownTime = 0
while shutDownTime < 1000000000:
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
            socket2.send_string("%s" % (string))
            continue
        # if other server failed, need remap
        if string.count(' ') == 0:
            # need remap
            if string in addStr:
                addStr.remove(string)
                ring = HashRing(addStr)
            continue
        # receive the message
        zipcode, temperature, relhumidity, strength, timePub = string.split()
        #####################################################################################
        print(zipcode)
        #####################################################################################
        server = ring.get_node(zipcode)
        if server != addStr[0]:
            continue
        else:
            # store the data
            # push in the new history
            zipcodeArrayHis.put(zipNewHis)
            temperatureArrayHis.put(temNewHis)
            relhumidityArrayHis.put(relNewHis)

            # pop up the old history
            zipcodeArrayHis.get()
            temperatureArrayHis.get()
            relhumidityArrayHis.get()

            # next time this should be pushed in history queue
            zipNewHis = int(zipcode)
            temNewHis = int(temperature)
            relNewHis = int(relhumidity)

            # copy queue
            zipcodeArrayHisT.queue = copy.deepcopy(zipcodeArrayHis.queue)
            temperatureArrayHisT.queue = copy.deepcopy(temperatureArrayHis.queue)
            relhumidityArrayHisT.queue = copy.deepcopy(relhumidityArrayHis.queue)

            # History
            # change to list and then use '/' as delimiter to make a string
            tempt = [zipcodeArrayHisT.get(), zipcodeArrayHisT.get(), zipcodeArrayHisT.get(), zipcodeArrayHisT.get(),
                     zipcodeArrayHisT.get()]
            zipHis = '/'.join(str(e) for e in tempt)
            tempt = [temperatureArrayHisT.get(), temperatureArrayHisT.get(), temperatureArrayHisT.get(),
                     temperatureArrayHisT.get(), temperatureArrayHisT.get()]
            temHis = '/'.join(str(e) for e in tempt)
            tempt = [relhumidityArrayHisT.get(), relhumidityArrayHisT.get(), relhumidityArrayHisT.get(),
                     relhumidityArrayHisT.get(), relhumidityArrayHisT.get()]
            relHis = '/'.join(str(e) for e in tempt)

            # store the message in class array
            hisList.append(History(zipcode, temperature, relhumidity, int(strength), float(timePub), zipHis, temHis, relHis))

            i += 1

    # get rid of the repeated topic hisList elements.
    # get all the zipcode
    zipList = [hisList[0].zipcode, hisList[1].zipcode, hisList[2].zipcode, hisList[3].zipcode, hisList[4].zipcode]

    # get all the strength
    strengList = [hisList[0].strength, hisList[1].strength, hisList[2].strength, hisList[3].strength,
                  hisList[4].strength]
    # if there is any element same
    # if not empty, that means there are same element
    retZipList = checksame(zipList)
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
            maxindex = getmax(indexList, strengList)
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
            socket2.send_string("%s" % (sys.argv[1]))
        # send last 5 infor (if repeated, not send)
        socket2.send_string("%i %i %i %i %f %s %s %s" % (
            his.zipcode, his.temperature, his.relhumidity, his.strength, his.timePub, his.zipHis, his.temHis, his.relHis))
        print ("send message")

    if shutDownTime == randnum:
        break
