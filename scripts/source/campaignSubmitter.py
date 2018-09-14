#!/usr/bin/env python

# This script is for running LQCD test jobs through PanDA
#

import os, sys, time, json, subprocess, random, re, datetime, logging, traceback
import yaml
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),"source"))
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from alchemybase import Base, session_scope, engine
from models.job import Job
from models.campaign import Campaign
from submitter import SequentialLQCDSubmitter
#You have to draw the line somewhere.
from termcolor import colored as coloured

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

def submitCampaign(Session,campSpecFile,listFile):


    QUEUE_NAME = 'ANALY_TJLAB_LQCD'
    VO = 'Gluex'
    # read yaml description

    jobdef = None

    try:
        campdef = PandaJobsYAMLParser.parse(campSpecFile)
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
            sls = SequentialLQCDSubmitter(aSrvID, QUEUE_NAME, VO)
            job = sls.createJob(walltime=walltime, command=jobCommand, outputFile=jobOutput, nodes=nodes, queuename=queuename, iterable=iterable, campaignID=campaign.id)
            sls.addJob(job.jobName, job) 
            print(iterable.strip()+", "+sls.submit(Session)+"\n")
    return None
