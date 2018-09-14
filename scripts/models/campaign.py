import datetime, os, sys, subprocess, logging, traceback
from sqlalchemy import Column, Integer, String, Interval, DateTime, JSON, event, ForeignKey, UniqueConstraint, asc, desc
from sqlalchemy.orm import relationship, mapper, joinedload
from sqlalchemy.inspection import inspect
from sqlalchemy.event import listen
from alchemybase import Base
import Client
from job import Job
#You have to draw the line somewhere.
from termcolor import colored as coloured

class Campaign(Base):
    __tablename__ = 'campaigns'
    __name__ = 'campaign'
        
    id = Column(Integer, primary_key=True)
    jobs = relationship("Job", back_populates="campaign",cascade="all, delete-orphan")
    name = Column('name',String,nullable=False,unique=True)
    lastUpdate = Column('lastUpdate',DateTime)

    def statusReport(self,Session,options=None):
        printStr = "\n Status Report for Campaign: "+self.name+"\n"
        printStr += "Last updated: "+str(self.lastUpdate)
        printStr += str(len(self.jobs))+" total jobs\n\n"
        for site in Session.query(Job.computingSite, Job.campaignID).filter(Job.campaignID == self.id).group_by(Job.computingSite).all():
            if site[0] and (site[0] != "NULL"):
                cS = site[0]
            else:
                cS = "Not yet allocated to a site"
            siteCount = str(Session.query(Job).filter(Job.computingSite.like(site[0])).count())
            printStr += cS + ": " + siteCount + "\n\n"

        for status in Session.query(Job.status,Job.subStatus, Job.campaignID).filter(Job.campaignID == self.id).group_by(Job.status,Job.subStatus).all():
            if (status[0] == "finished"):
                colour = 'green'
            if (status[0] == "failed"):
                colour = 'red'
            else:
                colour = 'blue'
            jobsThisStatus = Session.query(Job).filter(Job.status.like(status[0]),Job.subStatus.like(status[1]))
            statCount = str(jobsThisStatus.count())
            mostRecent = str(jobsThisStatus.order_by(desc('stateChangeTime')).first().stateChangeTime)
            oldest = str(jobsThisStatus.order_by(asc('stateChangeTime')).first().stateChangeTime)
            printStr += coloured("Status: %s \nSubStatus: %s \nCount: %s\nMost Recent: %s\nOldest: %s\n" % (status[0],status[1],statCount,mostRecent,oldest),colour)
        return printStr

    def updateJobs(self,Session):
        jobs_to_query = []
        for j in self.jobs:
            if j.status not in ["failed", "finished"]:
                jobs_to_query.append(j.pandaID)
        o = Client.getJobStatus(jobs_to_query)
        if (o):
            for j in o[1]:
                try:
                    dbJob = Session.query(Job).filter(Job.pandaID == j.PandaID).one()
                    dbJob.updateFromJobSpec(j)
                    Session.commit()
                except Exception as e:
                    logging.error(traceback.format_exc())
                    Session.rollback()
            self.lastUpdate = datetime.datetime.now()
        print(self.statusReport(Session))