import unittest
import os
from sqlalchemy_utils import database_exists
from storage.storage import Storage
from esofile_reader import EsoFile


class TestStorage(unittest.TestCase):
    ROOT = os.path.dirname(os.path.abspath(__file__))

    @classmethod
    def setUpClass(cls) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_create_db_in_memory(self):
        _ = Storage(echo=False)
        self.assertTrue(database_exists("sqlite:///:memory:"))

    def test_create_db(self):
        s = Storage("test.db", echo=False)
        self.assertTrue(database_exists("sqlite:///test.db"))
        s.delete_db()

    def test_create_db_full_path(self):
        path = os.path.join(self.ROOT, "test.db")
        s = Storage(path, echo=False)
        self.assertTrue(database_exists(f"sqlite:///{path}"))
        s.delete_db()

    def test_store_files(self):
        eso_file = EsoFile(self.ROOT, report_progress=False)
        s = Storage("test.db", echo=False)
        s.store_files(eso_file)



