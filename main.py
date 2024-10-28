from pathlib import Path
import sqlite3
from datetime import datetime
import os

DB_LOCATION = "./tracker.db"
LOGS_LOCATION = "./samples"

# Normally I would use pydantic and alembic to abstract this
# BUT we want to have no depenencies outside of python so this is how we are going to do it
class DB:
    # Create the tables that we are going to need
    # We don't really do version management since server wipes will wipe all data here as well
    # Schema changes can be introduced in that interval
    def __init__(self) -> None:
        dbfile = DB_LOCATION
        self.con = sqlite3.connect(dbfile)

        cur = self.con.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS "logs" (
                "id"	INTEGER,
                "entity_id"	INTEGER NOT NULL,
                "entity_name"	TEXT NOT NULL,
                "player_name"	TEXT NOT NULL,
                "datetime"	TEXT,
                "log"	TEXT NOT NULL,
                "epoch"	INTEGER,
                "file_id"	INTEGER NOT NULL,
                PRIMARY KEY("id" AUTOINCREMENT)
            )""")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS "files" (
                "id"	INTEGER,
                "file_path"	TEXT NOT NULL,
                "file_offset"	INTEGER NOT NULL,
                PRIMARY KEY("id" AUTOINCREMENT)
            )""")

        self.con.commit()
    
    # Check if the file already exists, if so only update the offset
    def upsert_file(self, fp: str, offset: int):
        cursor = self.con.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE file_path=?", (fp,))
        row_count = cursor.fetchone()[0]

        sql = "UPDATE files SET file_offset=? WHERE file_path=?"
        if (row_count == 0):
            sql = "INSERT INTO files(file_offset, file_path) VALUES (?,?)"

        cursor.execute(sql, (offset, fp))
        self.con.commit()
    
    # Add the newly found log
    def add_log(self, entity_id: int, entity_name: str, player_name: str, datetime: str, epoch: int, log: str, file_id: int):
        sql = """INSERT INTO logs(entity_id, entity_name, player_name, datetime, epoch, log, file_id) VALUES (?,?,?,?,?,?,?) """

        self.con.cursor().execute(sql, (entity_id, entity_name, player_name, datetime, epoch, log, file_id))
        self.con.commit()


    # If the file offset is not found it means we have never seen it so we will read it from the beginning 
    def get_file_offset(self, fp: str):
        offset = 0

        cursor = self.con.cursor()
        cursor.execute("SELECT file_offset FROM files WHERE file_path=?", (fp,))

        result = cursor.fetchone()

        if (result):
            offset = result[0]

        return offset
        
    # Returns file id, if not found make a file with offset 0 so we have a file id we can reference in the log line
    def get_file_id(self, fp: str):
        cursor = self.con.cursor()
        cursor.execute("SELECT id FROM files WHERE file_path=?", (fp,))
        row_id = cursor.fetchone()

        if (row_id):
            return row_id[0]

        sql = "INSERT INTO files(file_offset, file_path) VALUES (?,?)"

        cursor.execute(sql, (0, fp))
        self.con.commit()
        return cursor.lastrowid

class Importer:
    def __init__(self) -> None:
        self.db = DB()

    # List all files in the directory except for blacklisted names.
    # The blacklist looks at the START of the file
    def list_files(self, directory: str) -> list[str]:
        blacklist = ["deathlog"]
        return [
            str(file) for file in Path(directory).rglob('*') 
            if file.is_file() and not any(file.name.startswith(blacklisted_name) for blacklisted_name in blacklist)
        ]
    
    # The main function to parse the logs
    def parse_file(self, fp: str, offset: int = 0) -> None:
        # Get the filename without the extension
        filename = Path(os.path.basename(fp)).stem
        # Split the filename to entity name and id
        [entity_name, entity_id] = filename.rsplit("_", 1)

        # Ensure we already have a file id since we want to link the log to the file 
        file_id = self.db.get_file_id(fp)

        with open(fp, "rb") as log_file:
            # Seek the file to last offset (so we skip the data we already seen)
            log_file.seek(offset)
            for b_line in log_file:
                line = b_line.decode("utf-8")

                # Ugly hack to test with data that doesn't have a timestamp. Can be removed later :)
                if (line.startswith("[")):
                    timestamp = line[1:20]
                    epoch =  int(datetime.strptime(timestamp, "%m/%d/%Y %H:%M:%S").timestamp())

                    log = str(line)[22:]
                else:
                    timestamp = "empty line"
                    epoch = 0
                    log = str(line)

                # Split the log in to who and what. The _ variable is the split char in this case the space
                [who, _, what] = log.partition(" ")

                # Write the log to the database
                self.db.add_log(entity_id, entity_name, who, timestamp, epoch, what, file_id)

            # Since we are done reading the file, update the file offset
            self.db.upsert_file(fp, log_file.tell())

    def import_new_data(self):
        files = self.list_files(LOGS_LOCATION)
        for file in files:
            offset = self.db.get_file_offset(file)
            self.parse_file(file, offset)


importer = Importer()
importer.import_new_data()