#!/usr/bin/env python


import os, sys, time, json, subprocess, random, re, logging, traceback, yaml

from models.campaign import Campaign
#You have to draw the line somewhere.
from termcolor import colored as coloured

def deleteCampaign(Session,campName):

    try:
        campaign = Session.query(Campaign).filter(Campaign.name.like(campName)).first()
        if (campaign is None):
            print(coloured("No campaign of name "+campName+" found. Currently defined campaigns are: \n"),"red")
            for c in Session.query(Campaign.name).all():
                print(c[0])
            sys.exit(1)
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)

    answer = 'y'
    #answer = input('Really delete campaign all jobs for '+campaign.name+'?: [y/n]')

    if not answer or answer[0].lower() != 'y':
        return 'Aborting'
    else:
        return campaign.deleteJobs(Session)