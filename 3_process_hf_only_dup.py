import collections
import multiprocessing
import pickle

from tqdm import tqdm

import utils


def find_dup_cases_worker_func(info):
  input_path, case_ids = info
  cases = collections.defaultdict(list)
  for data in utils.read_jsonl_from_gz(input_path):
    case_text = data["详情"]
    case_id = utils.extract_case_id_hf(case_text)
    if case_id not in case_ids:
      continue
    cases[case_id].append(case_text)
  return cases


def find_dup_cases():
  """Find all the cases with case ids appear twice or more in the hugging face source.

  Returns:
    collections.defaultdict(list): key is case id and val is the set of case text corresponding
    to the case id.
  """
  data_folder = utils.get_cache_path()
  cache_path = data_folder / "hf_only_dup_cases.pkl"
  if cache_path.exists():
    with open(cache_path, "rb") as fp:
      return pickle.load(fp)

  # read in hf only duplicate case ids
  hf_only_dup = utils.load_case_ids(data_folder / "hf_only_dup.txt")

  # prepare workloads to process parallelly
  hf_gz_paths = utils.get_huggingface_gz_paths()
  workloads = []
  for path in hf_gz_paths:
    workloads.append((path, hf_only_dup))

  # accelerate the process with multiprocessing
  cpu_count = multiprocessing.cpu_count()
  results = collections.defaultdict(list)
  with multiprocessing.Pool(cpu_count) as pool:
    res_iter = tqdm(
      pool.imap_unordered(find_dup_cases_worker_func, workloads),
      total=len(workloads),
      desc="寻找huggingface源案号出现多次的案件"
    )
    for res in res_iter:
      for key, val in res.items():
        results[key].extend(val)

  # save the results for potential use later
  with open(cache_path, "wb") as fp:
    pickle.dump(results, fp)

  return results


def merger_worker_func(info):
  case_id, case_texts, queue = info
  case_texts = [utils.unescape(text) for text in case_texts]
  case_texts = utils.merge_texts(case_texts)
  for text in case_texts:
    output = utils.convert_case(case_id, text)
    output = utils.json_dump_bytes(output)
    queue.put(output)
  return (case_id, case_texts)


def compute_len(cases):
  return sum(len(v) for _, v in cases)


if __name__ == "__main__":
  results_folder = utils.get_results_path()

  dup_cases = find_dup_cases()
  dup_cases = list(dup_cases.items())
  print(f"合并前文书总数：{compute_len(dup_cases):,}")
  
  hf_result_path = results_folder / "hf_only_dup"
  if not hf_result_path.exists():
    hf_result_path.mkdir()
  cpu_count = multiprocessing.cpu_count()
  queue = multiprocessing.Manager().Queue()
  writer = utils.MPWriter(hf_result_path, queue=queue)
  writer.start()
  dup_cases = [
    (k, v, queue)
    for k, v in dup_cases
  ]
  with multiprocessing.Pool(cpu_count) as pool:
    dup_cases = list(tqdm(
      pool.imap_unordered(merger_worker_func, dup_cases),
      total=len(dup_cases),
      desc="合并并补全文书"
    ))
  print(f"合并后文书总数：{compute_len(dup_cases):,}")

  writer.close()


