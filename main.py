from storage.storage import Storage
from esofile_reader import EsoFile, Variable

if __name__ == "__main__":
    eso_file = EsoFile(r"tests/eso_files/eplusout_all_intervals.eso",
                       report_progress=False)
    storage = Storage("test.db")
    # eso_file.rename("foo")
    # storage.store_files(eso_file)

    statement = """SELECT file_path, variables.var_id FROM esofiles
     JOIN variables
     ON esofiles.id=variables.file_id
     WHERE variables.interval 
     IN ('hourly', 'timestep')
     """

    r = storage.fetch_variables("foo", [Variable("hourly", 'BLOCK1:ZONE1', None, None)])
    print(r)