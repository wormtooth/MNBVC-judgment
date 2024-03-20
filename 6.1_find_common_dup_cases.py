import multiprocessing
import sqlite3
import utils

def create_new_db(path):
  if path.exists():
    path.unlink()
  conn = sqlite3.connect(path)
  cur = conn.cursor()
  cur.execute("CREATE TABLE cases(case_id TEXT, case_text TEXT)")
  # cur.execute("PRAGMA journal_mode = MEMORY")
  # cur.execute("PRAGMA synchronous = OFF")
  conn.commit()
  return conn


def get_case_ids():
  path = utils.get_cache_path() / "hf_bd_common.txt"
  case_ids = utils.load_case_ids(path)
  return case_ids


def find_dup_cases_hf(input_path, case_ids):
  for data in utils.read_jsonl_from_gz(input_path):
    case_text = data["详情"]
    case_id = utils.extract_case_id_hf(case_text)
    if case_id not in case_ids:
      continue
    yield (case_id, case_text)


def find_dup_cases_bd(zip_path, csv_name, case_ids):

  for df in utils.read_csv_from_zip(zip_path, csv_name, pwd=b"253874"):
    for case_id, case_text in zip(df["案号"], df["全文"]):
      if type(case_id) is not str:
        case_id = "others"
      if case_id not in case_ids:
        continue
      if type(case_text) is not str:
        continue
      case_text = utils.clean_bd_case_text(case_text)
      if not case_text:
        continue
      yield (case_id, case_text)

def find_dup_cases(workloads, progress):
  process_idx = int(multiprocessing.current_process().name)
  case_ids = get_case_ids()

  # db settings
  db_path = utils.get_cache_path() / f"hf_bd_common_cases_{process_idx:02d}.db"
  conn = create_new_db(db_path)
  cur = conn.cursor()
  buffer_limit = 1000
  buffer_count = 0

  while True:
    workload = workloads.get()
    if workload is None:
      break
    data_gen = None
    if len(workload) == 1:
      input_path = workload[0]
      data_gen = find_dup_cases_hf(input_path=input_path, case_ids=case_ids)
    else:
      zip_path, csv_name = workload
      data_gen = find_dup_cases_bd(zip_path=zip_path, csv_name=csv_name, case_ids=case_ids)
    for data in data_gen:
      cur.execute("INSERT INTO cases VALUES (?, ?)", data)
      buffer_count += 1
      if buffer_count >= buffer_limit:
        conn.commit()
        buffer_count = 0
    progress.put(1)
  
  conn.commit()
  # create index for faster queries
  cur.execute("CREATE INDEX case_id_idx ON cases(case_id)")
  conn.commit()
  conn.close()


if __name__ == "__main__":

  worker_count = 4 # 共同案号加载完～3.5GB，根据电脑内存调整。最大可以设为 内存（GB）/ 4

  workloads = multiprocessing.Queue()
  progress = multiprocessing.Queue()

  # workloads from hugging face
  hf_gz_paths = utils.get_huggingface_gz_paths()
  for workload in hf_gz_paths:
    workloads.put((workload, ))

  # workloads from baidu
  zip_csv_paths = utils.get_baidu_zip_csv_paths()
  for workload in zip_csv_paths:
    workloads.put(workload)
  
  # progress bar
  bar = utils.ProgressBar(progress, desc="寻找在两个源出现的案件", total=len(hf_gz_paths) + len(zip_csv_paths))
  processes = []
  for idx in range(worker_count):
    processes.append(multiprocessing.Process(
      target=find_dup_cases, args=(workloads, progress), name=f"{idx + 1}"
    ))
    workloads.put(None)
  
  bar.start()
  for proc in processes:
    proc.start()
  
  for proc in processes:
    proc.join()
  bar.join()