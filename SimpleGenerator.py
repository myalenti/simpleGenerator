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
import json
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
bulkSize=20
username=""
password=""
database="axa"
collection="profiles"
logLevel=logging.INFO
procCount=2
totalDocuments=1000
tstart_time = time.time()
executionTimes=[]

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
    randSequence = random.sample(xrange(9999),30)
    faker = Factory.create()
    faker.seed(os.getpid())
    fakeText = faker.text()
    firstname = names.get_first_name()
    lastname = names.get_last_name()
    email = firstname + "." + lastname + "@mongodb.com"
    record = OrderedDict()
    record['name']  = OrderedDict()
    record['name']['firstName'] = firstname
    record['name']['lastName'] = lastname
    record['contact'] = OrderedDict()
    record['contact']['email'] = email
    record['nfld1'] = randSequence[0]
    record['nfld2'] = randSequence[2]
    record['nfld3'] = randSequence[3]
    record['nfld4'] = randSequence[4]
    record['nfld5'] = randSequence[5]
    record['nfld6'] = randSequence[6]
    record['nfld7'] = randSequence[7]
    record['nfld8'] = randSequence[8]
    record['nfld9'] = randSequence[9]
    record['nfld10'] = randSequence[10]
    record['nfld11'] = randSequence[11]
    record['nfld12'] = randSequence[12]
    record['nfld13'] = randSequence[13]
    record['nfld14'] = randSequence[14]
    record['nfld15'] = randSequence[15]
    record['nfld16'] = randSequence[16]
    record['nfld17'] = randSequence[17]
    record['nfld18'] = randSequence[18]
    record['nfld19'] = randSequence[19]
    record['nfld20'] = randSequence[20]
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
