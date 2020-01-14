import traceback

from sqlalchemy import (create_engine, exc)
from esofile_reader import EsoFile
from storage.models import DBEsoFile, DBVariable, Base
from utils.utils import merge_df_values
from sqlalchemy.orm import sessionmaker
from typing import List, Union, Tuple, Any
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

    def _create_db_variables(self, file: EsoFile, db_file: DBEsoFile):
        """ Store output variables. """
        db_variables = []
        for key in file.available_intervals:
            df = file.as_df(key)
            # parse results DataFrame to store values as blob
            sr = merge_df_values(df, separator="\t")

            for index, values in sr.iteritems():
                id_, interval, key, variable, units = index
                db_variables.append(
                    DBVariable(var_id=id_, interval=interval, key=key,
                               variable=variable, units=units, values=values,
                               file=db_file)
                )
        return db_variables

    def store_files(self, files: Union[List[EsoFile], EsoFile]) -> None:
        """ Store result files. """
        files = files if isinstance(files, list) else [files]
        for file in files:
            db_file = DBEsoFile(
                file_path=file.file_path,
                file_name=file.file_name,
                file_timestamp=datetime.fromtimestamp(file.file_timestamp),
                complete=file.complete
            )
            db_variables = self._create_db_variables(file, db_file)

            # add each file separately to avoid complete
            # failure when only one of files would fail
            self.session.add(db_file)
            self.session.add_all(db_variables)
            try:
                self.session.commit()
            except exc.IntegrityError:
                print(f"Cannot add file {db_file}!"
                      f"\nINTEGRITY ERROR"
                      f"\n{traceback.format_exc()}")

    def execute_statement(self, statement: str) -> List[Any]:
        """ Execute sql statement. """
        with self.engine.connect() as con:
            rs = con.execute(statement)
            data = rs.fetchall()
            return data
