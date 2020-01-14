from storage.storage import Storage
from esofile_reader import EsoFile

if __name__ == "__main__":
    eso_file = EsoFile(r"tests/eso_files/eplusout_all_intervals.eso",
                       report_progress=False)
    storage = Storage("test.db")
    #
    storage.store_files(eso_file)

    statement = """SELECT file_path, variables.var_id FROM esofiles
     JOIN variables
     ON esofiles.id=variables.file_id
     """
    r = storage.execute_statement(statement)
    print(r)
