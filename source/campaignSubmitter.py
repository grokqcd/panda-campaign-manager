import os, sys, re, logging, traceback, datetime
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from models.job import Job
from models.campaign import Campaign
#You have to draw the line somewhere.
from termcolor import colored as coloured
import submissionTools

def submitCampaign(Session,campSpecFile,listFile):


    # read yaml description

    jobdef = None

    try:
        campdef = submissionTools.PandaJobsYAMLParser.parse(campSpecFile)
        campaign = Session.query(Campaign).filter(Campaign.name.like(campdef['campaign'])).first()
        if (campaign is None):
            campaign = Campaign(name=campdef['campaign'],lastUpdate=datetime.datetime.now())
            Session.add(campaign)
            Session.commit()
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)


    aSrvID = None

    nodes = campdef['jobtemplate']['nodes']
    walltime = campdef['jobtemplate']['walltime']
    queuename = campdef['jobtemplate']['queuename']
    try:
        outputFile = campdef['jobtemplate']['outputFile']
    except:
        outputFile = None
    command = campdef['jobtemplate']['outputFile']

    with open(listFile,'r') as f:
        for iterable in f:
            jobCommand = re.sub('(<iter>)',iterable,command)
            jobOutput = re.sub('(<iter>)',iterable,outputFile)
            dbJob = Job(script=command,iterable=iterable,nodes=nodes,wallTime=walltime,status="Submitted",campaignID=campaign.id,outputFile=outputFile)
            jobSpec = submissionTools.createJobSpec(walltime=walltime, command=jobCommand, outputFile=jobOutput, nodes=nodes, campaignID=campaign.id)
            dbJob.servername = jobSpec.jobName
            s,o = Client.submitJobs([jobSpec])
            try:
                dbJob.pandaID = o[0][0]
                dbJob.status = 'submitted'
                dbJob.subStatus = 'submitted'
                print(coloured(iterable.strip()+", "+str(o[0][0])+"\n",'green'))
            except Exception as e:
                logging.error(traceback.format_exc())
                print o
                print(coloured(iterable.strip()+" job failed to submit\n",'red'))
                dbJob.status = 'failed'
                dbJob.subStatus = 'failed'
            Session.add(dbJob)
            Session.commit()
    return None
