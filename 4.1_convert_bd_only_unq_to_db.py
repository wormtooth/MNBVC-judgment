import sqlite3

import utils
from tqdm import tqdm

if __name__ == "__main__":
  data_path = utils.get_cache_path()
  input_path = data_path / "bd_only_unq.txt"
  case_ids = utils.load_case_ids(input_path)

  db_path = data_path / "bd_only_unq.db"
  if db_path.exists():
    db_path.unlink()
  con = sqlite3.connect(db_path)
  
  cur = con.cursor()
  cur.execute("CREATE TABLE case_id(case_id TEXT)")
  con.commit()
  
  buffer = 0
  for case_id in tqdm(case_ids):
    cur.execute("INSERT INTO case_id VALUES (?)", (case_id, ))
    buffer += 1
    if buffer == 1000:
      con.commit()
      buffer = 0

  con.commit()
  cur.execute("CREATE INDEX case_id_idx on case_id(case_id)")
  con.commit()
  con.close()