from sqlalchemy import (Column, Integer, String, Boolean, DateTime, ForeignKey,
                        Sequence)

from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DBEsoFile(Base):
    __tablename__ = 'esofiles'
    id = Column(Integer, Sequence('db_id_seq'), primary_key=True)
    file_path = Column(String(120))
    file_name = Column(String(50), unique=True)
    file_timestamp = Column(DateTime)
    complete = Column(Boolean)
    variables = relationship("DBVariable", backref="file")

    def __repr__(self):
        return f"<EsoFile (filepath='{self.file_path}', filename='{self.file_name}', " \
            f"complete='{self.complete}', timestamp='{self.file_timestamp}'')>"


class DBVariable(Base):
    __tablename__ = 'variables'
    id = Column(Integer, Sequence('var_id_seq'), primary_key=True)
    var_id = Column(Integer)
    file_id = Column(Integer, ForeignKey('esofiles.id'))
    interval = Column(String(120), index=True)
    key = Column(String(120), index=True)
    variable = Column(String(120), index=True)
    units = Column(String(50), index=True)
    values = Column(String)

    def __repr__(self):
        return f"<Variable (interval='{self.interval}', key='{self.key}', " \
            f"variable='{self.variable}', units='{self.units}'')>"
