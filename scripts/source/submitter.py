#!/usr/bin/env python

# This script is for running LQCD test jobs through PanDA
#

import os
import yaml
import sys
import time
import json
import subprocess
import random
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from models.job import Job

QUEUE_NAME = 'ANALY_TJLAB_LQCD'
VO = 'Gluex'


class SequentialLQCDSubmitter:
    transformation = '#json#'
    datasetName = 'panda.destDB.%s' % subprocess.check_output('uuidgen')
    destName    = 'local'
    prodSourceLabel = 'user'
    currentPriority = 1000
    __debug = False

    def __init__(self, aSrvID, site, vo):
        self.__aSrvID = aSrvID
        self.__joblist = []
        self.site = site
        self.vo = vo
        self.dbJob = Job()

    def createJob(self, nodes, walltime, command, iterable, campaignID, outputFile=None, queuename = None):
        job = JobSpec()
        job.jobDefinitionID   = int(time.time()) % 10000
        job.jobName           = "%s" % subprocess.check_output('uuidgen')
        job.VO = self.vo
        job.transformation = self.transformation

        job.destinationDBlock = self.datasetName
        job.destinationSE     = self.destName
        job.currentPriority   = self.currentPriority
        job.prodSourceLabel   = self.prodSourceLabel
        job.computingSite     = self.site if queuename is None else queuename

        self.dbJob = Job(script=command,iterable=iterable,nodes=nodes,wallTime=walltime,status="Submitted",campaignID=campaignID,serverName=job.jobName)

        lqcd_command = {
                "nodes" : nodes,
                "walltime" : walltime,
                "name" : job.jobName,
                "command" : command
                }

        if(outputFile):
            lqcd_command['outputFile'] = outputFile
            self.dbJob.outputFile = outputFile
        job.jobParameters = json.dumps(lqcd_command)

        fileOL = FileSpec()
        fileOL.lfn = "%s.job.log.tgz" % job.jobName.strip()
        fileOL.destinationDBlock = job.destinationDBlock
        fileOL.destinationSE     = job.destinationSE
        fileOL.dataset           = job.destinationDBlock
        fileOL.type = 'log'

        job.addFile(fileOL)

        job.cmtConfig = None

        return job

    def addJob(self, name, job_desc, nextJob=None):
        if name in self.__joblist:
            return False
        if job_desc.cmtConfig==name: # job depends on itself
            return False
        job_desc.cmtConfig = json.dumps({'name' : name, 'next' : nextJob})
        #self.__joblist[name] = [job_desc, None]
        self.__joblist.append(job_desc)
        return True

    def submit(self,Session):
        if self.__debug:
            for i in self.__joblist:
                print(i.cmtConfig)
        return self.__submit(Session)

    def __submit(self,Session):
        s,o = Client.submitJobs(self.__joblist)
        try:
            self.dbJob.pandaID = o[0][0]
            self.dbJob.status = 'Submitted'
            self.dbJob.subStatus = 'Submitted'
            Session.add(self.dbJob)
            Session.commit()
        except:
           print("Job failed to submit")
           Session.rollback()
        return str(o[0][0])