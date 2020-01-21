import traceback
import pandas as pd
import os
import contextlib

from sqlalchemy import (create_engine, exc, and_)
from esofile_reader import EsoFile, Variable
from storage.models import DBEsoFile, DBVariable, Base
from utils.utils import merge_df_values, destringify_df, profile
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

    Constants
    ---------
    SEPARATOR
        A string to specify separator in values data blob.

    """
    SEPARATOR = "\t"

    def __init__(self, path: str = None, echo: bool = True, **kwargs):
        if not path:
            # create database in memory
            path = ":memory:"

        # TODO create Config class to hold database details

        # create SQL base objects
        self.path = path
        self.engine = create_engine(f'sqlite:///{path}', echo=echo, **kwargs)
        self.create_tables()

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        Session = sessionmaker(bind=self.engine)
        session = Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise Exception
        finally:
            session.close()

    def _create_db_variables(self, file: EsoFile, db_file: DBEsoFile):
        """ Store output variables. """
        db_variables = []
        for key in file.available_intervals:
            df = file.as_df(key)
            # parse results DataFrame to store values as blob
            sr = merge_df_values(df, separator=self.SEPARATOR)

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

            with self.session_scope() as session:
                # failure when only one of files would fail
                session.add(db_file)
                session.add_all(db_variables)

    def fetch_file(self, file_name: str) -> EsoFile:
        """ Fetch results file from database. """
        with self.session_scope() as session:
            q = session.query(DBEsoFile).filter(DBEsoFile.file_name == file_name)
            if q.count() > 0:
                return q[0]

    @staticmethod
    def _construct_variable_condition(variable: Variable):
        """
        Create ORM sql query for results variable.

        Filter condition is applied only if the value is specified.

        Example
        -------
        Variable(interval=None, key='Level 1', variable='Temperature', units='C')

        will fetch variable with key='Level 1', variable='Temperature', units='C'
        for all intervals.

        """

        return {k: v for k, v in variable._asdict().items() if v}

    @profile
    def fetch_variables(self, file_name: str, variables: List[Variable]) -> pd.DataFrame:
        """
        Fetch variables

        Arguments
        ---------
        file_name : str
            A specific results file,
        variables : list of (Variable)
            A list of Variable namedtuples

        Notes
        -----
        'Variable' name tuple can be imported from 'esofile_reader' package.

        Returns
        -------
        pd.DataFrame
        """

        # ORM query
        orm_columns = (
            DBVariable.var_id, DBVariable.interval, DBVariable.key,
            DBVariable.variable, DBVariable.units, DBVariable.values
        )

        # no need for JOIN, ORM handles this automatically
        with self.session_scope() as session:
            q = session \
                .query(*orm_columns) \
                .filter(DBEsoFile.file_name == file_name)

        rs = []
        for variable in variables:
            cond = self._construct_variable_condition(variable)
            sq = q.filter_by(**cond)
            if sq.count() > 0:
                rs.extend(sq.all())

        if rs:
            # convert results into pd.DataFrame, values are stored in rows
            df = pd.DataFrame(rs)
            df.set_index(["var_id", "interval", "key", "variable", "units"], inplace=True)
            df = destringify_df(df, separator=self.SEPARATOR)

            # transform df to get data as columns
            df.astype(float, copy=False)

            return df

    def execute_statement(self, statement: str) -> List[Any]:
        """ Execute sql statement. """
        with self.engine.connect() as con:
            rs = con.execute(statement)
            return rs.fetchall()

    def delete_db(self):
        """ Delete database file from file system. """
        try:
            os.remove(self.path)
        except OSError:
            pass

    def create_tables(self):
        """ Create all tables. """
        Base.metadata.create_all(self.engine)

    def drop_tables(self):
        """ Drop all tables. """
        Base.metadata.drop_all()
