from os import listdir, remove
from os.path import isfile, join, stat
import random
import string
from time import sleep
import datetime
import math


def logstats(stats):
    #logs the given statistics dictionary to he compations log file
    compactionlog = "compactions.log"
    with open(compactionlog, "a") as compaction_log_file:
        compaction_log_file.write(str(datetime.datetime.now()) + \
                                  " : " + \
                                  str(stats) + \
                                  "\n")

def compact(oldlogfile):
    lognum = str(oldlogfile[4]) # extract log number
    currlogfile = "redo"+lognum+"_1.log"
    stats = {}
    num_compactions, old_count, curr_count = (0,0,0) #Stats

    with open(oldlogfile, 'r') as oldlogin:
        with open(currlogfile, 'w') as currlogout:
            contents = {}
            oldlogin.seek(0)
            lines = oldlogin.readlines()
            # read key value pairs, compact and writeout
            for l in lines:
                arr = l.split(",")
                (key, value) = (arr[0],arr[1])
                if contents.keys().__contains__(key):
                    num_compactions += 1 # Stats
                contents[key] = value # store most recent version
                old_count += 1 # Stats

            # write contents out
            for k,v in contents.items():
                currlogout.write(k+","+v)
                curr_count += 1 # Stats

            stats[oldlogfile] = [old_count, num_compactions, curr_count]

    logstats(stats) # Stats



def combinelogs(redologscompacted):
    contents = {}
    stats = {}
    data = []
    num_compactions, old_count, curr_count, numfiles = (0 ,0, 0, 0)

    # get all key values in compacted files
    for oldlogfile in redologscompacted:
        with open(oldlogfile, 'r') as oldlogin:
            for l in oldlogin.readlines():
                arr = l.split(",")
                (key, value) = (arr[0],arr[1])
                if contents.keys().__contains__(key):
                    num_compactions += 1 # Stats
                contents[key] = value # store most recent version
                old_count += 1 # Stats

    # convert contents to data for easy partitioning
    for k,v in contents.items():
        data.append(str(k)+","+str(v))

    curr_count = len(data) #Stats

    # redistribute compacted values into logs
    # get number of files needed and place
    # 100 items into each file
    numfiles = math.ceil(len(data) / 100) # Stats
    for filenum in range(numfiles):
        currlogfile = "redo"+str(filenum)+".log"
        with open(currlogfile, 'w') as currlogout:
            for linenum in range(100*filenum, 100*(filenum+1)):
                try:
                    currlogout.write(data[linenum])
                except(IndexError):
                    pass # end of contents reached

    stats["multifile"] = [old_count, num_compactions, curr_count, numfiles] # Stats

    logstats(stats)


def generateredologs():
    # generate redo logs (DEBUG)
    # should be: db writes to redo0 always
    # and rolls data back into the next available
    # redo log number, so when compaction,
    # compact never touches redo 0,
    # compact only works on redo 1+
    # and compacts into redo1
    numlogs = random.randint(1,10)
    for num in range(1,numlogs):
        with open("redo"+str(num-1)+".log", 'w') as redolog:
            for i in range(0,99):
                key = random.choice(string.ascii_lowercase) + \
                      str(random.randint(0,9))
                value = random.randint(0,1000)
                redolog.write(key + "," + str(value)+"\n")


def removefiles(filelist):
    # remove old log files
    numfiles = len(filelist)
    for num in range(numfiles):
        rmfile = filelist[num]
        sleep(1)
        remove(rmfile)
        sleep(1)


def getfilelist(logpath, namecontains):
    # returns a list of file containing the given string in the file name
    filenames = [f for f in listdir(logpath) if isfile(join(logpath, f))]
    logs = [f for f in filenames if namecontains in f]
    return logs



if __name__=="__main__":
    logpath = "."

    #DEBUG: generate the redo log files
    generateredologs()

    # get all log files
    redolist = getfilelist(logpath, "redo")

    # compact each logfile
    for logfile in redolist:
        compact(logfile)
        sleep(1)

    # remove old redo logs
    removefiles(redolist)

    # combine compacted logfiles into one file
    compactedlist = getfilelist(logpath, "_")

    # combine compacted files into filled redo log files
    combinelogs(compactedlist)

    # remove old compacted log files
    removefiles(compactedlist)


