import random
import threading
import time
import logging
import pymongo
import sys
import timeit
import multiprocessing
import getopt
import ast
import names
import random
import os
import numpy
from faker import Factory
from collections import OrderedDict

from pymongo import MongoClient, InsertOne

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

#preGenerating random list for use in document with a random integer
randomList=[]
for i in xrange(22):
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

def generateDocument():
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
    firstname = "fn"
    lastname = "ln"
    email = "fn_ln" + "@mongodb.com"
    record = OrderedDict()
    record['name']  = OrderedDict()
    record['name']['firstName'] = firstname
    record['name']['lastName'] = lastname
    record['contact'] = OrderedDict()
    record['contact']['email'] = email
    record['nfld1'] = randAnchor / randomList[0]
    record['nfld2'] = randAnchor / randomList[1]
    record['nfld3'] = randAnchor / randomList[2]
    record['nfld4'] = randAnchor / randomList[3]
    record['nfld5'] = randAnchor / randomList[4]
    record['nfld6'] = randAnchor / randomList[5]
    record['nfld7'] = randAnchor / randomList[6]
    record['nfld8'] = randAnchor / randomList[7]
    record['nfld9'] = randAnchor / randomList[8]
    record['nfld10'] = randAnchor / randomList[9]
    record['nfld11'] = randAnchor / randomList[10]
    record['nfld12'] = randAnchor / randomList[11]
    record['nfld13'] = randAnchor / randomList[12]
    record['nfld14'] = randAnchor / randomList[13]
    record['nfld15'] = randAnchor / randomList[14]
    record['nfld16'] = randAnchor / randomList[15]
    record['nfld17'] = randAnchor / randomList[16]
    record['nfld18'] = randAnchor / randomList[17]
    record['nfld19'] = randAnchor / randomList[18]
    record['nfld20'] = randAnchor / randomList[19]
    #record['nfld21'] = randSequence[21]
    #record['nfld22'] = randSequence[22]
    #record['nfld23'] = randSequence[23]
    #record['nfld24'] = randSequence[24]
    #record['nfld25'] = randSequence[25]
    #record['nfld26'] = randSequence[26]
    #record['nfld27'] = randSequence[27]
    #record['nfld28'] = randSequence[28]
    #record['nfld29'] = randSequence[29]
    #record['nfld30'] = randSequence[1]
    #record['strfld1'] = fakeText[0:20]
    #record['strfld2'] = fakeText[21:40]
    #record['strfld3'] = fakeText[41:60]
    #record['strfld4'] = fakeText[61:80]
    #record['strfld5'] = fakeText[81:100]
    #record['strfld6'] = fakeText[101:120]
    #record['strfld7'] = fakeText[121:140]
    #record['strfld8'] = fakeText[141:160]
    #record['strfld9'] = fakeText[161:180]
    #record['strfld10'] = fakeText[181:200]

    #print record
    logging.debug( "Document : %s" % (str(record)))
    return record

def worker(iterations):
    connection = connector()
    db = connection[database]
    col = db[collection]

    itCounter = 0

    while ( itCounter < iterations):
        logging.debug("Entering Iterator")
        counter = 0
        request = []
        while (counter < bulkSize):
            logging.debug("Entering Bulk Counter")
            request.append(InsertOne(generateDocument()))
            counter += 1
        try:
            logging.debug("%s" % str(request))
            start_time = time.time()
            bulk_result = col.bulk_write(request)
            end_time = time.time()
            execTime = end_time - start_time
            logging.debug("%s" % str(bulk_result.bulk_api_result))
            logging.info("Executed in : %s" % str(execTime))
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

#worker(100)
jobs = []

#setSize is the quotient of totaldocument/procCount (how many docs each process is to deliver)
setSize = totalDocuments / procCount
#Iterations is the number of times the worker function has to execute to deliver the total number of docs in a sets
bulkCount = setSize/bulkSize
logging.info("Set Size is %d, and bulk count is %d given bulk size of %d" % (setSize, bulkCount, bulkSize))
for i in range(procCount):
    p = multiprocessing.Process(target=worker, args=(bulkCount,))
    p.start()
    jobs.append(p)

for i in jobs:
    i.join()
tend_time = time.time()
logging.info("Total Execution time was %f" % (tend_time - tstart_time) )
