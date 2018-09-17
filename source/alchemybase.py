import os,sys
from contextlib import contextmanager
from sqlalchemy import create_engine,event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
dbFile = os.path.split(os.path.abspath(__file__))[0]+"/local.db"
engine = create_engine('sqlite:///'+dbFile,echo=False)

def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute('pragma foreign_keys=ON')

event.listen(engine, 'connect', _fk_pragma_on_connect)

@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    DBSession = sessionmaker(bind=engine)
    Session = DBSession()
    try:
        yield Session
        Session.commit()
    except:
        Session.rollback()
        raise
    finally:
        Session.close()