import sqlite3
import multiprocessing
import utils


class SqliteDataset:

  def __init__(self):
    connections = []
    cache_folder = utils.get_cache_path()
    db_paths = cache_folder.glob("hf_bd_common_cases*.db")
    for db_path in db_paths:
      conn = sqlite3.connect(db_path)
      connections.append(conn)
    self.connections = connections
    self.cursors = [
      conn.cursor()
      for conn in connections
    ]
  
  def get(self, key):
    ret = []
    for cursor in self.cursors:
      sql = "SELECT case_text FROM cases WHERE case_id = ?"
      for row in cursor.execute(sql, (key, )).fetchall():
        ret.append(row[0])
    return ret
  
  def close(self):
    for conn in self.connections:
      conn.close()


def merger_feeder_func(workloads):

  with open(utils.get_cache_path() / "hf_bd_common.txt", "r") as fp:
    for line in fp:
      line = line.strip()
      if not line:
        continue
      workloads.put(line)


def merger_worker_func(workloads, progress):

  data = SqliteDataset()
  file_idx = int(multiprocessing.current_process().name)
  file_idx = 1000 * file_idx
  results_folder = utils.get_results_path() / "hf_bd_common"
  writer = utils.SizeLimitedFile()
  writer.open(results_folder / f"{file_idx}.jsonl")
  file_idx += 1
  
  while True:
    workload = workloads.get()
    if workload is None:
      break
    case_id = workload
    texts = data.get(case_id)
    texts = [utils.unescape(text) for text in texts]
    texts.sort(key=lambda t: -t.count("\n")) # make hf source first
    texts = utils.merge_texts(texts)
    for text in texts:
      output = utils.convert_case(case_id, text)
      output = utils.json_dump_bytes(output)
      writer.writeline(output)
      if writer.is_full():
        writer.close()
        writer.open(results_folder / f"{file_idx}.jsonl")
        file_idx += 1
    progress.put(1)

  writer.close()
  data.close()


if __name__ == "__main__":

  results_folder = utils.get_results_path() / "hf_bd_common"
  if not results_folder.exists():
    results_folder.mkdir()

  workloads = multiprocessing.Queue()
  progress = multiprocessing.Queue()

  # prepare workloads
  workloads_count = 0
  with open(utils.get_cache_path() / "hf_bd_common.txt", "r") as fp:
    for line in fp:
      line = line.strip()
      if not line:
        continue
      workloads_count += 1
  
  feeder = multiprocessing.Process(
    target=merger_feeder_func,
    args=(workloads,)
  )

  worker_count = multiprocessing.cpu_count()
  processes = []
  for idx in range(worker_count):
    proc = multiprocessing.Process(
      target=merger_worker_func,
      args=(workloads, progress),
      name=f"{idx + 1}"
    )
    processes.append(proc)
  bar = utils.ProgressBar(progress, desc="处理在两个源同时出现的文书", total=workloads_count)

  bar.start()
  for proc in processes:
    proc.start()
  feeder.start()

  feeder.join()
  for _ in range(worker_count):
    workloads.put(None)

  for proc in processes:
    proc.join()
  bar.join()