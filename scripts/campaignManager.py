#!/usr/bin/env python

#!/usr/bin/env python

# This script is for running LQCD test jobs through PanDA
#

import argparse, sys
from source.alchemybase import Base, session_scope, engine
from source.campaignUpdater import updateCampaign
from source.campaignStatus import statusCampaign
from source.campaignSubmitter import submitCampaign

with session_scope(engine) as Session:
    Base.metadata.create_all(engine)
    
    def submitCampaignWrap(args):
        submitCampaign(Session,args.template,args.list)

    def updateCampaignWrap(args):
        print(updateCampaign(Session,args.CampaignName))

    def statusCampaignWrap(args):
        print(statusCampaign(Session,args.CampaignName))

    def deleteCampaignWrap(args):
        pass

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
    
    delete = subparser.add_parser("Delete", help="Delete a campaign")
    delete.add_argument("CampaignName",help="Campaign to delete")
    delete.set_defaults(func=deleteCampaignWrap) 

    args = parser.parse_args()
    args.func(args)

    sys.exit(0)
