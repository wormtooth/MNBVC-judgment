import collections
import multiprocessing

from tqdm import tqdm

import utils


def count_case_id_hf(path) -> collections.Counter:
  case_id_set = collections.Counter()
  for data in utils.read_jsonl_from_gz(path):
    text = data["详情"]
    case_id = utils.extract_case_id_hf(text)
    case_id_set[case_id] += 1
  return case_id_set


def count_case_id_bd(info) -> collections.Counter:
  zip_path, csv_mbr = info
  data = utils.read_csv_from_zip(
    zip_path, csv_mbr, pwd=b"253874", chunk_size=1000)
  case_id_set = collections.Counter()
  for df in data:
    df = df[~df["全文"].isna()]
    for case_id, case_text in zip(df["案号"], df["全文"]):
      if type(case_text) is not str:
        continue
      case_text = case_text.strip()
      if not case_text:
        continue
      if type(case_id) is not str:
        case_id = "others"  # "missing" is used in hugging face source
      case_id = case_id.strip()
      if not case_id:
        case_id = "others"
      case_id_set[case_id] += 1
  return case_id_set


def filter_case_ids(included, excluded, prop, cache_path):
  count = 0
  with open(cache_path, "w") as fp:
    for case_id, case_count in tqdm(included.items()):
      if case_id in excluded:
        continue
      if not prop(case_count):
        continue
      fp.write(case_id)
      fp.write("\n")
      count += 1
  return count


if __name__ == "__main__":
  results_path = utils.get_cache_path()
  if not results_path.exists():
    results_path.mkdir()
  cpu_count = multiprocessing.cpu_count()

  # case_id from huggingface
  hf_gz_paths = utils.get_huggingface_gz_paths()
  with multiprocessing.Pool(cpu_count) as pool:
    results = list(
      tqdm(
        pool.imap(count_case_id_hf, hf_gz_paths),
        total=len(hf_gz_paths),
        desc="处理huggingface源的案号"
      )
    )
  hf_case_ids = results.pop()
  while results:
    for key, val in results.pop().items():
      hf_case_ids[key] += val

  # case id from baidupan
  bd_zip_csv_paths = utils.get_baidu_zip_csv_paths()
  cnt = 0
  with multiprocessing.Pool(cpu_count) as pool:
    results = list(
      tqdm(
        pool.imap(count_case_id_bd, bd_zip_csv_paths),
        total=len(bd_zip_csv_paths),
        desc="处理百度网盘源的案号"
      )
    )
  bd_case_ids = results.pop()
  while results:
    for key, val in results.pop().items():
      bd_case_ids[key] += val

  count = filter_case_ids(
    hf_case_ids, bd_case_ids, lambda v: v == 1,
    results_path / "hf_only_unq.txt",
  )
  print(f"仅在huggingface源出现一次的案号数量：{count}")

  count = filter_case_ids(
    hf_case_ids, bd_case_ids, lambda v: v > 1,
    results_path / "hf_only_dup.txt"
  )
  print(f"仅在huggingface源出现多次的案号数量：{count}")

  count = filter_case_ids(
    bd_case_ids, hf_case_ids, lambda v: v == 1,
    results_path / "bd_only_unq.txt"
  )
  print(f"仅在百度网盘源出现一次的案号数量：{count}")

  count = filter_case_ids(
    bd_case_ids, hf_case_ids, lambda v: v > 1,
    results_path / "bd_only_dup.txt"
  )
  print(f"仅在百度网盘源出现多次的案号数量：{count}")

  # find the common set
  count = 0
  with open(results_path / "hf_bd_common.txt", "w") as fp:
    for case_id in tqdm(hf_case_ids):
      if case_id not in bd_case_ids:
        continue
      count += 1
      fp.write(case_id)
      fp.write("\n")
  print(f"共同案号数量: {count}")
