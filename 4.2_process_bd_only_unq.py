import multiprocessing

import utils


def load_case_ids():
  data_folder = utils.get_cache_path()
  case_ids = utils.SqliteSet(data_folder / "bd_only_unq.db")
  return case_ids

def worker_func(workloads, progress):
  process = multiprocessing.current_process()
  file_index = 1000 * (int(process.name) + 1)
  case_ids = load_case_ids()
  writer = utils.SizeLimitedFile(file_size_limit=500_000_000)
  results_folder = utils.get_results_path() / "bd_only_unq"
  writer.open(results_folder / f"{file_index}.jsonl")
  file_index += 1
  
  while True:
    workload = workloads.get()
    if workload is None:
      break
    zip_path, csv_name = workload
    for df in utils.read_csv_from_zip(zip_path, csv_name, pwd=b"253874", chunk_size=1000):
      for case_id, case_text in zip(df["案号"], df["全文"]):
        if type(case_id) is not str:
          case_id = "others"
        case_id = case_id.strip()
        if case_id not in case_ids:
          continue
        if type(case_text) is not str:
          continue
        case_text = utils.clean_bd_case_text(case_text)
        case_text = utils.unescape(case_text)
        if not case_text:
          continue
        output = utils.convert_case(case_id, case_text)
        output = utils.json_dump_bytes(output)
        writer.writeline(output)
        if writer.is_full():
          writer.close()
          writer.open(results_folder / f"{file_index}.jsonl")
          file_index += 1
    progress.put(1)
  
  writer.close()
  case_ids.close()

if __name__ == "__main__":
  results_folder = utils.get_results_path() / "bd_only_unq"
  if not results_folder.exists():
    results_folder.mkdir()
  
  # prepare workloads to process parallelly
  csv_paths = utils.get_baidu_zip_csv_paths()
  workloads = multiprocessing.Queue()
  for path in csv_paths:
    workloads.put(path)

  progress = multiprocessing.Queue()

  # worker
  processes = []
  worker_count = multiprocessing.cpu_count() # 应少于CPU核的数量
  for idx in range(worker_count):
    processes.append(
      multiprocessing.Process(target=worker_func, args=(workloads, progress), name=f"{idx}")
    )
    workloads.put(None)
  for proc in processes:
    proc.start()

  # progress bar
  bar = utils.ProgressBar(progress, desc="处理仅在百度网盘源出现一次的案件", total=len(csv_paths))
  bar.start()
  
  for proc in processes:
    proc.join()
  bar.join()