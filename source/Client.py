import os, sys, commands, pickle, logging, traceback, json
from Curl import Curl


try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandawms.org:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://pandawms.org:25443/server/panda'
try:
    baseURLMonitor = os.environ['PANDA_URL_MONITOR']
except:
    baseURLMonitor = 'http://pandamonitor.org/latest/jobs/?'

# default max size per job
maxTotalSize = long(14*1024*1024*1024)

# safety size for input size calculation
safetySize = long(500*1024*1024)

# limit on maxCpuCount
maxCpuCountLimit = 1000000000

userName = "Daniel%20Trewartha"

# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    print("No valid grid proxy certificate found")
    return ''

# submit jobs
def submitJobs(jobs,verbose=False):
    # set hostname
    hostname = commands.getoutput('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/submitJobs'
    data = {'jobs':strJobs}
    curl.verifyHost = False
    status,output = curl.post(url,data)
    if status!=0:
        print(output)
        return status,None
    try:
        return status,pickle.loads(output)
    except Exception as e:
        logging.error(traceback.format_exc())
        return 1,[]

# get job status
def getJobStatus(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = Curl()
    # execute
    url = baseURL + '/getJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except Exception as e:
        logging.error(traceback.format_exc())
        return 1,[]

# kill jobs
def killJobs(ids,verbose=False):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/killJobs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except Exception as e:
        logging.error(traceback.format_exc())
        return 1,[]

def getAllJobs():
    #Get job ids and names from the server to repopulate the database.
    curl = Curl()
    curl.verbose=False
    headers = ['Content-Type: application/json','Accept: application/json']
    url = '\''+baseURLMonitor+"user=%s\'" % userName
    output = curl.get(url,{},headers=headers)
    return output