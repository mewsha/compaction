from os import listdir, remove
from os.path import isfile, join, stat
import random
import string
from time import sleep
import sys
import math
import datetime
from prometheus_client import start_http_server, Gauge


OLD_COUNT = Gauge('log_compact_old_count', "additional data")
NUM_COMPACTIONS = Gauge('log_compact_num_compactions', "additional data")
CURR_COUNT = Gauge('log_compact_curr_count', "additional data")
NUM_FILES = Gauge('log_compact_num_files', "additional data")


def logstats(stats):
    #logs the given statistics to the compactions log file
    OLD_COUNT.set(stats[0])
    NUM_COMPACTIONS.set(stats[1])
    CURR_COUNT.set(stats[2])
    NUM_FILES.set(stats[3])


def compact(logpath, oldlogfile):
    filename = oldlogfile.strip(".log")
    lognum = str(filename[4:])# extract log number
    currlogfile = logpath+"redo"+lognum+"_1.log"
    stats = {}
    num_compactions, old_count, curr_count = (0,0,0) #Stats

    with open(logpath+oldlogfile, 'r') as oldlogin:
        with open(currlogfile, 'w') as currlogout:
            contents = {}
            oldlogin.seek(0)
            line = oldlogin.readlines()[0].rstrip(',').lstrip('{')
            arr = line.split(",")
            print(arr)
            # read key value pairs, compact and writeout
            #formatted {key:value,key2,key:value}
            for a in arr:
                p = a.split(":")
                try:
                    (key, value) = (p[0],p[1])
                    if contents.keys().__contains__(key):
                        num_compactions += 1 # Stats
                    contents[key] = value # store most recent version
                    old_count += 1 # Stats
                except IndexError as e:
                    pass

            # write contents out
            currlogout.write("{")
            numitems = len(contents.items())
            for k,v in contents.items():
                currlogout.write(k+":"+v)
                curr_count += 1 # Stats
                currlogout.write(",")

            stats[oldlogfile] = [old_count, num_compactions, curr_count]

    # optionally log stats for each file
    # logstats(stats) # Stats



def combinelogs(logpath,redologscompacted):
    contents = {}
    stats = []
    data = []
    num_compactions, old_count, curr_count, numfiles = (0 ,0, 0, 0)

    # get all key values in compacted files
    for oldlogfile in redologscompacted:
        with open(logpath+oldlogfile, 'r') as oldlogin:
            line = oldlogin.readlines()[0].rstrip(',').lstrip('{')
            arr = line.split(",")
            # read key value pairs, compact and writeout
            #formatted {key:value,key2,key:value}
            for a in arr:
                p = a.split(":")
                try:
                    (key, value) = (p[0],p[1])
                    if contents.keys().__contains__(key):
                        num_compactions += 1 # Stats
                    contents[key] = value # store most recent version
                    old_count += 1 # Stats
                except IndexError as e:
                    pass

    # convert contents to data for easy partitioning
    for k,v in contents.items():
        data.append(str(k)+":"+str(v))

    curr_count = len(data) #Stats

    # redistribute compacted values into logs
    # get number of files needed and place
    # 100 items into each file
    print(curr_count/100.0)
    print(math.ceil(len(data) / 100.0))
    numfiles = int(math.ceil(len(data) / 100.0)) # Stats
    print(numfiles)
    print("---")

    #print statistics
    compactionlog = logpath+"compactions.log"
    with open(compactionlog, "a") as compaction_log_file:
        compaction_log_file.write(str(datetime.datetime.now()) + \
                                  " : " + \
                                  str(curr_count/100.0) + \
                                  " : " + \
                                  str(math.ceil(len(data) / 100.0)) + \
                                  " : " + \
                                  str(numfiles) + \
                                  "\n")

    for filenum in range(numfiles):
        currlogfile = logpath+"redo"+str(filenum)+".log"
        with open(currlogfile, 'w') as currlogout:
            currlogout.write("{")
            for linenum in range(100*filenum, 100*(filenum+1)):
                try:
                    currlogout.write(data[linenum])
                    currlogout.write(",")
                except(IndexError):
                    pass # end of contents reached
    stats = [old_count, num_compactions, curr_count, numfiles] # Stats

    # log stats for combining all log files
    logstats(stats)


def generateredologs(logpath):
    # generate redo logs (DEBUG)
    # should be: db writes to last redo file always
    # compact never touches last redo log,
    # compact only works on redo 0-max-1
    # and compacts into redo0+
    numlogs = random.randint(1,10)
    for num in range(1,numlogs):
        with open(logpath+"redo"+str(num-1)+".log", 'w') as redolog:
            redolog.write("{")
            for i in range(0,100):
                key = random.choice(string.ascii_lowercase) + \
                      str(random.randint(0,9))
                value = random.randint(0,1000)

                redolog.write(key + ":" + str(value))
                redolog.write(",")


def removefiles(logpath, filelist):
    # remove old log files
    numfiles = len(filelist)
    for num in range(numfiles):
        rmfile = filelist[num]
        remove(logpath+rmfile)


def getfilelist(logpath, namecontains):
    # returns a list of file containing the given string in the file name
    filenames = [f for f in listdir(logpath) if isfile(join(logpath, f))]
    logs = [f for f in filenames if namecontains in f]
    return logs



if __name__=="__main__":
    logpath = "./logs/"

    # Start up the server to expose the metrics.
    start_http_server(9101)

    if sys.argv[0] == 'test':
        print(logpath)
    else:
        while True:
            sleep(60)
            #DEBUG: generate the redo log files
            #generateredologs(logpath)

            # get all log files
            redolist = getfilelist(logpath, "redo")
            print(redolist)

            #exclude last file from list (currently in use by database)
            # loop through redo list
            maxNum = 0
            for logfile in redolist:
                # get number at end of file, all files format "redo##.log"
                filename = logfile.strip(".log")
                filenum = int(filename[4:])
                maxNum = max(filenum, maxNum)
            # remove last file from redo list
            excludeFile = "redo"+str(maxNum)+".log"
            redolist.remove(excludeFile)
            print(redolist)

            # compact each logfile
            for logfile in redolist:
                compact(logpath, logfile)


            # remove old redo logs
            removefiles(logpath, redolist)

            # combine compacted logfiles into as few files as possible
            compactedlist = getfilelist(logpath, "_")

            # combine compacted files into filled redo log files
            combinelogs(logpath, compactedlist)

            # remove old compacted log files
            removefiles(logpath, compactedlist)


