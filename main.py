from storage.storage import Storage
from esofile_reader import EsoFile, Variable

if __name__ == "__main__":
    eso_file = EsoFile(r"tests/eso_files/eplusout_all_intervals.eso",
                       report_progress=False)
    storage = Storage("test.db", echo=False)
    # storage.store_files(eso_file)
    storage.fetch_variables("eplusout_all_intervals",
                            [Variable("timestep", None, "Zone People Occupant Count", "")])
