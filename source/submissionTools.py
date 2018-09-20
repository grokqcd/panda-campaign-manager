import os, sys, json, yaml, subprocess, time
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

QUEUE_NAME = 'ANALY_TJLAB_LQCD'
VO = 'Gluex'

def createJobSpec(nodes, walltime, command, jobName, outputFile=None):

    transformation = '#json#'
    datasetName = 'panda.destDB.%s' % subprocess.check_output('uuidgen')
    destName    = 'local'
    prodSourceLabel = 'user'
    currentPriority = 1000

    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = jobName
    job.VO = VO
    job.transformation = transformation

    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = currentPriority
    job.prodSourceLabel   = prodSourceLabel
    job.computingSite     = QUEUE_NAME
    job.cmtConfig = json.dumps({'name' : job.jobName, 'next':None})
    lqcd_command = {
            "nodes" : nodes,
            "walltime" : walltime,
            "name" : job.jobName,
            "command" : command
            }

    if(outputFile):
        lqcd_command['outputFile'] = outputFile
    job.jobParameters = json.dumps(lqcd_command)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName.strip()
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'

    job.addFile(fileOL)

    #job.cmtConfig = None

    return job


class PandaJobsYAMLParser:
    def __init__(self):
        pass

    @staticmethod
    def parse(filename):
        if filename is None or filename == "":
            raise OSError("filename is empty")
        with open(filename, "r") as stream:
            try:
                return yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return None

