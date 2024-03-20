import multiprocessing

import utils


def load_case_ids():
  data_folder = utils.get_cache_path()
  path = data_folder / "hf_only_unq.txt"
  return utils.load_case_ids(path)

def worker_func(workloads, progress):
  process = multiprocessing.current_process()
  file_index = 1000 * (int(process.name) + 1)
  case_ids = load_case_ids()
  writer = utils.SizeLimitedFile(file_size_limit=500_000_000)
  results_folder = utils.get_results_path() / "hf_only_unq"
  writer.open(results_folder / f"{file_index}.jsonl")
  file_index += 1
  
  while True:
    workload = workloads.get()
    if workload is None:
      break
    input_path = workload
    for data in utils.read_jsonl_from_gz(input_path):
      case_text = utils.unescape(data["详情"])
      case_id = utils.extract_case_id_hf(case_text)
      if case_id not in case_ids:
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

if __name__ == "__main__":
  results_folder = utils.get_results_path() / "hf_only_unq"
  if not results_folder.exists():
    results_folder.mkdir()
  
  # prepare workloads to process parallelly
  hf_gz_paths = utils.get_huggingface_gz_paths()
  workloads = multiprocessing.Queue()
  for path in hf_gz_paths:
    workloads.put(path)

  progress = multiprocessing.Queue()

  # worker
  processes = []
  worker_count = multiprocessing.cpu_count() # 应少于CPU核的数量，注意：每个子进程使用约750MB内存
  for idx in range(worker_count):
    processes.append(
      multiprocessing.Process(target=worker_func, args=(workloads, progress), name=f"{idx}")
    )
    workloads.put(None)
  for proc in processes:
    proc.start()

  # progress bar
  bar = utils.ProgressBar(progress, desc="处理仅在huggingface出现一次的案件", total=len(hf_gz_paths))
  bar.start()
  
  for proc in processes:
    proc.join()
  bar.join()