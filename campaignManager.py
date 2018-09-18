#!/usr/bin/env python

import argparse, os, sys, logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(os.path.abspath(__file__))[0],'source')))
from alchemybase import Base, session_scope, engine
from campaignUpdater import updateCampaign
from campaignStatus import statusCampaign
from campaignSubmitter import submitCampaign
from campaignDeleter import deleteCampaign
from campaignResubmitter import resubmitCampaign

with session_scope(engine) as Session:
    Base.metadata.create_all(engine)
    logging.basicConfig(level=logging.WARNING)
    
    def submitCampaignWrap(args):
        #We don't just print this after completion - submission can take a while, and so progress is reported incrementally
        submitCampaign(Session,args.template,args.list)

    def updateCampaignWrap(args):
        print(updateCampaign(Session,args.CampaignName))

    def statusCampaignWrap(args):
        print(statusCampaign(Session,args.CampaignName))

    def deleteCampaignWrap(args):
        print(deleteCampaign(Session,args.CampaignName))

    def resubmitCampaignWrap(args):
        print(resubmitCampaign(Session,args.CampaignName,args.resubmit_cancelled))

    parser = argparse.ArgumentParser(description='Campaign submitter and manager for Panda')
    subparser = parser.add_subparsers()

    submit = subparser.add_parser("Submit", help="Submit a list of jobs from a job template and list file.")
    submit.add_argument("template",help="A job template file")
    submit.add_argument("list",help="A list of iterables")
    submit.set_defaults(func=submitCampaignWrap)

    update = subparser.add_parser("Update", help="Update the jobs in a campaign from the panda server")
    update.add_argument("CampaignName",help="Campaign to update")
    update.set_defaults(func=updateCampaignWrap) 

    status = subparser.add_parser("Status", help="Provide a status report from a campaign")
    status.add_argument("CampaignName",help="Campaign to report on")
    status.set_defaults(func=statusCampaignWrap) 
    
    resubmit= subparser.add_parser("Resubmit", help="Resubmit failed jobs")
    resubmit.add_argument("CampaignName",help="Campaign to resubmit")
    resubmit.add_argument('--resubmit-cancelled', nargs='?', default=False)
    resubmit.set_defaults(func=resubmitCampaignWrap) 

    delete = subparser.add_parser("Delete", help="Delete a campaign")
    delete.add_argument("CampaignName",help="Campaign to delete")
    delete.set_defaults(func=deleteCampaignWrap) 

    args = parser.parse_args()
    args.func(args)

    sys.exit(0)
