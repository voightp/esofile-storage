from sqlalchemy import (create_engine)
from esofile_reader import EsoFile
from storage.models import DBEsoFile, DBVariable, Base
from sqlalchemy.orm import sessionmaker
from typing import List, Union
from datetime import datetime


class Storage:
    """
    Class representing sqlite database.

    Parameters
    ----------
    path : str, default None
        A path to sqlite db file, db is created in memory if 'None'
    echo : bool, default True
        Activate sqlalchemy logging.
    **kwargs
        Keyword arguments passed to sql engine.

    Attributes
    ----------
    engine
        sqlalchemy engine object.
    session
        sqlalchemy orm session object.


    """

    def __init__(self, path: str = None, echo: bool = True, **kwargs):
        if not path:
            # create database in memory
            path = ":memory:"

        # create SQL base objects
        self.engine = create_engine(f'sqlite:///{path}', echo=echo, **kwargs)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)

        self.session = Session()

    def store_file_variables(self, file: EsoFile):
        """ Store output variables. """
        pass

    def store_files(self, files: Union[List[EsoFile], EsoFile]):
        """ Store result files. """
        files = files if isinstance(files, list) else [files]
        db_files = []
        for file in files:
            db_files.append(
                DBEsoFile(
                    file_path=file.file_path,
                    file_name=file.file_name,
                    file_timestamp=datetime.fromtimestamp(file.file_timestamp),
                    complete=file.complete)
            )
        self.session.add_all(db_files)
        self.session.commit()

    def execute_statement(self, statement: str):
        """ Execute sql statement. """
        with self.engine.connect() as con:
            rs = con.execute(statement)
            data = rs.fetchall()
            return data
