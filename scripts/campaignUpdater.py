#!/usr/bin/env python

# This script is for running LQCD test jobs through PanDA
#

import os, sys, time, json, subprocess, random, re, logging, traceback, yaml

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

with session_scope(engine) as Session:
    Base.metadata.create_all(engine)
    
    if len(sys.argv) < 2:
        print(coloured("No campaign specified",'red'))
        sys.exit(1)
    else:
        campName = sys.argv[1]

    try:
        campaign = Session.query(Campaign).filter(Campaign.name.like(campName)).first()
        if (campaign is None):
            print("No campaign of name "+campName+" found. Currently defined campaigns are: \n")
            for c in Session.query(Campaign.name).all():
                print(c[0])
            sys.exit(1)
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)

    campaign.updateJobs(Session)

    sys.exit(0)
