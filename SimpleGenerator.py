import random
import threading
import time
import logging
import sys
import timeit
import multiprocessing
import getopt
import ast
import names
import random
import os
import numpy
import datetime
import pymongo
from faker import Factory
from collections import OrderedDict
from time import sleep

from pymongo import MongoClient, InsertOne, command_cursor
from Finder.Finder_items import item

#Global Parameters
target="127.0.0.1"
port=27017
repSet="rpl1"
bulkSize=100
username=""
password=""
database="axa"
collection="profiles"
logLevel=logging.INFO
procCount=2
totalDocuments=1000
tstart_time = time.time()
executionTimes=[]
seqBase = 100000000
#opMode = "insert"
opMode = "workload"
workloadTime = 30
minSeqId = 0
maxSeqId = 0

faker = Factory.create()


#preGenerating random list for use in document with a random integer
randomList=[]
for i in xrange(5):
    randomList.append(random.random())


logging.basicConfig(level=logLevel,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )

def connector():
    try:
        #connection = MongoClient(target,port,replicaSet=repSet,serverSelectionTimeoutMS=2000,connectTimeoutMS=2000)
        connection = MongoClient(target,port,connectTimeoutMS=2000)
        if (username != ""):
            connection.admin.authenticate(username,password)
            return connection
        else :
            return connection
    except :
        print "connection failure, aborting"
        sys.exit()
    return

def generateDocument(seqId):
    #randSequence = random.sample(xrange(9999),21)
    randSeed = random.randint(0, 100)
    randConfuser = randomList[1]
    randAnchor = randSeed * randConfuser
    #faker = Factory.create()
    #faker.seed(os.getpid())
    #fakeText = faker.text()
    ####Generating data from Faker is epensive... Very expensive
    #firstname = names.get_first_name()
    #lastname = names.get_last_name()
    #email = firstname + "." + lastname + "@mongodb.com"
    #firstname = "fn"
    #lastname = "ln"
    #email = "fn_ln" + "@mongodb.com"
    record = OrderedDict()
    #Four Text Fields
    record['SeqId'] = seqId
    record['Text1'] = faker.word()
    record['Text2'] = faker.word()
    record['Text3'] = faker.word()
    
    #Nine Date/Time Fields
    record['Date1'] = datetime.datetime.utcnow()
    record['Date2'] = datetime.datetime.utcnow()
    record['Date3'] = datetime.datetime.utcnow()
    record['Date4'] = datetime.datetime.utcnow()
    record['Date5'] = datetime.datetime.utcnow()
    record['Date6'] = datetime.datetime.utcnow()
    record['Date7'] = datetime.datetime.utcnow()
    record['Date8'] = datetime.datetime.utcnow()
    record['Date9'] = datetime.datetime.utcnow()
    #Four Float Fields
    record["Float1"] = randAnchor / randomList[0]
    record["Float2"] = randAnchor / randomList[1]
    record["Float3"] = randAnchor / randomList[2]
    record["Float4"] = randAnchor / randomList[3]
    #Four Integer Fields
    record['Integer1'] = random.randint(0,100)
    record['Integer2'] = random.randint(0,200)
    record['Integer3'] = random.randint(0,300)
    record['Integer4'] = random.randint(0,400)
    #Four Bool fields
    bolList = []
    bolList.append(True)
    bolList.append(False)
    record['Bool1'] = random.choice( bolList)
    record['Bool2'] = random.choice( bolList)
    record['Bool3'] = random.choice( bolList)
    record['Bool4'] = random.choice( bolList)
    
    #print record
    logging.debug( "Document : %s" % (str(record)))
    return record

def worker(iterations, seqBase):
    connection = connector()
    db = connection[database]
    col = db[collection]

    itCounter = 0
    seqNumb = seqBase

    while ( itCounter < iterations):
        logging.debug("Entering Iterator")
        counter = 0
        request = []
        while (counter < bulkSize):
            logging.debug("Entering Bulk Counter")
            request.append(InsertOne(generateDocument(seqNumb)))
            counter += 1
            seqNumb += 1
        try:
            logging.debug("%s" % str(request))
            start_time = time.time()
            bulk_result = col.bulk_write(request)
            end_time = time.time()
            execTime = end_time - start_time
            logging.debug("%s" % str(bulk_result.bulk_api_result))
            logging.debug("Executed in : %s" % str(execTime))
            executionTimes.append(execTime)
            #print len(executionTimes)

        except pymongo.errors.BulkWriteError as e:
            #e = sys.exc_info()[0]
            print 'Bulk Error detected'
            print request
            print e.details
            #attrs = vars(e)
            #print ', '.join("%s: %s" % item for item in attrs.items())
            #print e
            exit()
        itCounter += 1
    logging.info("Average Bulk write execution time in secs %f on thread %s" % ( numpy.average(executionTimes), multiprocessing.current_process().pid))


def getMinMax():
    global minSeqId
    global maxSeqId
    connection = connector()
    db = connection[database]
    col = db[collection]
    cur = col.aggregate( [{ "$group" : { "_id" : {} , "max" : { "$max" : "$SeqId" }}}, { "$project" : { "_id" : 0 }} ])
    item = cur.next()
    maxSeqId = item['max']
    cur = col.aggregate( [{ "$group" : { "_id" : {} , "min" : { "$min" : "$SeqId" }}}, { "$project" : { "_id" : 0 }} ])
    item = cur.next()
    minSeqId = item['min']
    logging.info(" Max is %d and Min is %d" % (maxSeqId, minSeqId))

def wquery():
    connection = connector()
    db = connection[database]
    col = db[collection]  
    seqId = random.randint(minSeqId, maxSeqId)
    
    startTime = datetime.datetime.now()
    endTime = startTime + datetime.timedelta(seconds=workloadTime)
    print startTime
    print endTime
    
    while ( datetime.datetime.now() < endTime):
        query = { "SeqId" : seqId }
        cur = col.find( query )
        item = cur.next()
        logging.info("Query Element %s" % str(item))
        sleep(0.1)
    

def wupdate():
    connection = connector()
    db = connection[database]
    col = db[collection]  
    seqId = random.randint(minSeqId, maxSeqId)
    
    startTime = datetime.datetime.now()
    endTime = startTime + datetime.timedelta(seconds=workloadTime)
    print startTime
    print endTime
    
    while ( datetime.datetime.now() < endTime):
        newValue = random.randint(0,100)
        query = { "SeqId" : seqId }
        update = { "$set" : {"Integer2" : newValue }}
        result = col.update_one( query, update )
        logging.info("Update Results %s" % str(result.raw_result))
        sleep(0.1)

def slowInserts():
    nextSeqId = maxSeqId + 1
    connection = connector()
    db = connection[database]
    col = db[collection]  
    startTime = datetime.datetime.now()
    endTime = startTime + datetime.timedelta(seconds=workloadTime)
    print startTime
    print endTime
    
    while ( datetime.datetime.now() < endTime):
        result = col.insert_one(generateDocument(nextSeqId))
        logging.info("Update Results %s" % str(result.inserted_id))
        sleep(0.1)
        nextSeqId += 1
        
jobs = []

#setSize is the quotient of totaldocument/procCount (how many docs each process is to deliver)
setSize = totalDocuments / procCount
#Iterations is the number of times the worker function has to execute to deliver the total number of docs in a sets
bulkCount = setSize/bulkSize
procSeqBase = seqBase  

if (opMode == "insert"):
    logging.info("Working in Insert mode")
    logging.info("Set Size is %d, and iteration count is %d given bulk size of %d" % (setSize, bulkCount, bulkSize))
    for i in range(procCount):
        p = multiprocessing.Process(target=worker, args=(bulkCount,procSeqBase,))
        p.start()
        jobs.append(p)
        procSeqBase = seqBase + ( (i+1) * setSize)
        logging.info("Process id %d with base of %d"  % ( i , procSeqBase ))
  

    

if (opMode == "workload"):
    logging.info("Working in workload mode")
    getMinMax()
    p = multiprocessing.Process(target=wquery, args=())
    p.start()
    jobs.append(p)
    p = multiprocessing.Process(target=wupdate, args=())
    p.start()
    jobs.append(p)
    p = multiprocessing.Process(target=slowInserts, args=())
    p.start()
    jobs.append(p)
    #wquery()
    #wupdate()
    #slowInserts()

for i in jobs:
        i.join()
        tend_time = time.time()
        logging.info("Total Execution time was %f" % (tend_time - tstart_time) )
