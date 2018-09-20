#!/usr/bin/env python


import os, sys, time, json, subprocess, random, re, logging, traceback, yaml

from models.campaign import Campaign
from models.job import Job
import Client
#You have to draw the line somewhere.
from termcolor import colored as coloured

#Not functional right now
def repopulateDatabase(Session):

    """
    try:
        output = Client.getAllJobs()
        if output[0] != 0:
            raise Exception("Server error")
        else:
            output = json.loads(output[1])['jobs']
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)

    for j in output:
        try:
            #Check for pre-existing job with this pandaid
            if (not Session.query(Job).filter(Campaign.pandaID.like(j['pandaid'])).exists()):
                campaignName = j['jobname'][:-32]
                campaign = Session.query(Campaign).filter(Campaign.name.like(campaignName)).first()
                if (campaign is None):
                    campaign = Campaign(name=campdef['campaign'],lastUpdate=datetime.datetime.utcnow())
                    Session.add(campaign)
                    Session.commit()
                #We can't recover the job script from the monitor output
                job = Job(pandaID=j['pandaid'],script="unknown")
    """
    return None
