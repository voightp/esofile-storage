from sqlalchemy import (Column, Integer, String, Boolean, DateTime, ForeignKey,
                        Sequence)

from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DBEsoFile(Base):
    __tablename__ = 'esofiles'
    id = Column(Integer, Sequence('db_id_seq'), primary_key=True)
    file_path = Column(String)
    file_name = Column(String, unique=True)
    file_timestamp = Column(DateTime)
    complete = Column(Boolean)
    variables = relationship("DBVariable")

    def __repr__(self):
        return f"<EsoFile (filepath='{self.filepath}', filename='{self.filename}', " \
            f"complete='{self.complete}', timestamp='{self.timestamp}'')>"


class DBVariable(Base):
    __tablename__ = 'variables'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('esofiles.id'))
    interval = Column(String, index=True)
    key = Column(String, index=True)
    variable = Column(String, index=True)
    units = Column(String, index=True)
    values = Column(String)

    def __repr__(self):
        return f"<Variable (interval='{self.interval}', key='{self.key}', " \
            f"variable='{self.variable}', units='{self.units}'')>"
