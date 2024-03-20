import gzip
import hashlib
import json
import logging
import multiprocessing
import pathlib
import re
import sqlite3
import threading
import zipfile
from html import parser
from typing import Iterator, List, Tuple, Union

import pandas as pd
from tqdm import tqdm

from customSimhash import Simhash

logger = logging.getLogger(__name__)


def get_root_path() -> pathlib.Path:
  path = pathlib.Path(__file__).absolute().parent
  return path


def get_huggingface_data_path() -> pathlib.Path:
  """返回指向hugging face的裁判文书数据源。这个文件夹下应该有如下子文件夹：
  ├── 20230134
  ├── 20230135
  ├── 20230136
  ├── 20230137
  ├── 20230138
  ├── 20230139
  ├── 20230140
  └── 20230141

  每个子文件夹里存有压缩过的 jsonl 文件: *.jsonl.gz
  """
  path = get_root_path() / "data" / "裁判文书-huggingface"
  if not path.exists():
    raise Exception(
      "请从https://huggingface.co/datasets/liwu/MNBVC/tree/main/law/judgement下载裁判文书\n"
      f"并放置到{path}中，或者修改此函数返回指向正确数据的本地路径。"
    )
  return path


def get_baidu_data_path() -> pathlib.Path:
  """返回指向百度网盘的裁判文书数据源。这个文件夹下应该有1985~2021年的裁判文书 zip 包：xxxx年裁判文书数据.zip
  """

  path = get_root_path() / "data" / "裁判文书全量数据"
  if not path.exists():
    raise Exception(
      "请从百度网盘下载https://pan.baidu.com/share/init?surl=hzy4Pg62PeBHI4mtgx8DYA&pwd=4p46裁判文书\n"
      f"并放置到{path}中，或者修改此函数返回指向正确数据的本地路径。"
    )
  return path


def get_cache_path() -> pathlib.Path:
  path = get_root_path() / "cache"
  if not path.exists():
    path.mkdir()
  return path

def get_results_path() -> pathlib.Path:
  """返回用于存处理好的数据的文件夹。
  """
  path = get_root_path() / "data" / "results"
  if not path.exists():
    path.mkdir()
  return path


def get_huggingface_gz_paths() -> List[pathlib.Path]:
  """返回hugging face源中所有 jsonl.gz 文件的路径。
  """
  hf_source_path = get_huggingface_data_path()
  hf_gz_paths = [
    path
    for path in hf_source_path.glob("**/*.jsonl.gz")
    if "jsonl" in str(path)
  ]
  return hf_gz_paths


def get_baidu_zip_csv_paths() -> List[Tuple[pathlib.Path, str]]:
  """返回百度网盘中 zip 文件的路径以及 zip 文件中 csv 的文件名。
  """
  bd_source_path = get_baidu_data_path()
  zip_paths = list(bd_source_path.glob("**/*.zip"))
  csv_paths = []
  for path in zip_paths:
    with zipfile.ZipFile(path) as zf:
      for name in zf.namelist():
        if not name.endswith(".csv"):
          continue
        csv_paths.append((path, name))
  return csv_paths


def read_csv_from_zip(
    zip_path: Union[pathlib.Path, str], csv_name: str,
    pwd: bytes = None,
    chunk_size: int = 1000,
    **pd_kwargs
  ) -> Iterator[pd.DataFrame]:
  """读取 zip 文件中的 csv。

  Args:
      zip_path (Union[pathlib.Path, str]): zip 文件包的路径。
      csv_name (str): zip 文件包的 csv 文件名。
      pwd (bytes, optional): zip 文件包的解压密码. Defaults to None.
      chunk_size (int, optional): 读取 csv 每次最多读取的行数. Defaults to 1000.
      pd_kwargs: 其他参数将用于 pd.read_csv 中。

  Yields:
      pd.DataFrame: 
  """
  with zipfile.ZipFile(zip_path) as zf:
    with zf.open(csv_name, pwd=pwd) as fp:
      yield from pd.read_csv(fp, chunksize=chunk_size, **pd_kwargs)


def read_jsonl_from_gz(path: Union[pathlib.Path, str]) -> Iterator[dict]:
  """读取 jsonl.gz 文件。
  """
  with gzip.open(path) as gz:
    for line in gz:
      line = line.strip()
      if not line:
        continue
      try:
        yield json.loads(line)
      except:
        logger.error(f"bad line from {path}: {line}")


def read_jsonl(path: Union[pathlib.Path, str]) -> Iterator[dict]:
  """读取 jsonl.gz 文件。
  """
  with open(path, "r") as fp:
    for line in fp:
      line = line.strip()
      if not line:
        continue
      try:
        yield json.loads(line)
      except:
        logger.error(f"bad line from {path}: {line}")


def extract_case_id_hf(case_text: str) -> str:
  lines = [
    line.strip()
    for line in case_text.split("\n")
    if line.strip()
  ]
  if len(lines) == 1:
    return lines[0]

  ptn = r"(.?\d{0,4}.?.*?号)"
  for line in lines:
    ids = re.findall(ptn, line)
    if ids:
      return ids[0]

  return "missing"


def load_case_ids(path: Union[pathlib.Path, str]) -> set:
  with open(path, "r") as fp:
    ret = set(line.strip() for line in fp if line.strip())
  return ret


def convert_case(case_id: str, case_text: str):

  paras = case_text.split("\n")
  converted_lines = []
  max_len = -1

  hashes = set()
  texts = []
  
  
  for idx, line in enumerate(paras):
    line = line.strip()
    if not line:
      continue
    md5 = hashlib.md5(line.encode()).hexdigest()
    line_dict = {
      "行号": idx + 1,
      "是否重复": md5 in hashes,
      "是否跨文件重复": False,
      "md5": md5,
      "内容": line,
      "拓展字段": None
    }
    converted_lines.append(line_dict)
    max_len = max(max_len, len(line))

    # 去重
    if md5 not in hashes:
      texts.append(line)
    hashes.add(md5)
  
  # 如果没有任何内容，返回None
  if len(hashes) == 0:
    return None
  
  simhash_val = Simhash(texts).value

  output = {
    "文件名": case_id,
    "是否待查文件": False,
    "是否重复文件": False,
    "文件大小": len(case_text),
    "simhash": simhash_val,
    "最长段落长度": max_len,
    "段落数": len(converted_lines),
    "去重段落数": len(hashes),
    "低质量段落数": 0,
    "段落": converted_lines,
    "拓展字段": None,
    "时间": extract_time(case_id, case_text)
  }

  return output


def json_dump_bytes(obj):
  """将 json 序列号成 bytes。
  """
  ret = json.dumps(obj, ensure_ascii=False).encode()
  return ret


class SizeLimitedFile:
  """用于写入限制大小的文件。

  使用场景：可以使用此类生成大小需要限制在 500 MB 左右的 jsonl 文件。
  """

  def __init__(self, file_size_limit=500_000_000):
    self.file_size_limit = file_size_limit
    self.current_size = 0
    self.fp = None

  def open(self, path):
    self.close()
    self.fp = open(path, "wb")
    self.current_size = 0

  def close(self):
    if self.fp is not None:
      self.fp.close()
      self.fp = None
      self.current_size = 0

  def is_full(self):
    return self.current_size >= self.file_size_limit

  def write(self, data, force=False):
    if self.fp is None:
      raise Exception("No file open for write")
    if self.is_full() and (not force):
      raise Exception(
        f"File size {self.current_size:,} bytes, exceeding limit {self.file_size_limit:,} bytes")

    if type(data) is str:
      data = data.encode()
    self.current_size += self.fp.write(data)

  def writeline(self, data):
    self.write(data)
    self.write(b"\n", force=True)

  def __del__(self):
    self.close()


class MPWriter:
  """接受其他进程的数据 （bytes），将其写入文件。

  此类主要用于写入 jsonl 文件。其他进程负责处理文本，转化成 json 格式，转化成 bytes 数据，然后使用此类写到本地存储中。
  使用时，可以限定 `file_size_limit` 来限制输出文件的大小。默认为 500MB。
  """

  def __init__(self, output_folder, queue, file_size_limit=500_000_000, worker_num=1):
    if type(output_folder) is str:
      output_folder = pathlib.Path(output_folder)
    self.output_folder = output_folder

    self.queue = queue
    self.file_size_limit = file_size_limit
    self.worker_num = worker_num
    self.workers = [
      threading.Thread(target=self.worker_func, args=(queue, ))
      for _ in range(worker_num)
    ]

    self.get_file_path_lock = multiprocessing.Lock()
    self.file_count = 0

  def start(self):
    for t in self.workers:
      t.start()

  def close(self):
    for _ in range(self.worker_num):
      self.queue.put(None)
    for t in self.workers:
      t.join()

  def _next_output_path(self):
    self.file_count += 1
    path = self.output_folder / f"{self.file_count}.jsonl"
    return path

  def get_next_output_path(self):
    self.get_file_path_lock.acquire()
    path = self._next_output_path()
    self.get_file_path_lock.release()

    return path

  def worker_func(self, queue):
    path = self.get_next_output_path()
    fp = SizeLimitedFile(file_size_limit=self.file_size_limit)
    fp.open(path)

    while True:
      data = queue.get()
      if data is None:
        break
      fp.writeline(data)
      if fp.is_full():
        fp.close()
        path = self.get_next_output_path()
        fp.open(path)

    fp.close()


class ProgressBar(threading.Thread):
  """在多线程或多进程的场景下使用 ProgressBar 来显示任务进度。
  """

  def __init__(self, queue, **tqdm_kwargs):
    super().__init__()
    self.queue = queue
    self.bar = tqdm(**tqdm_kwargs)

  def join(self):
    self.queue.put(None)
    super().join()

  def run(self):
    while True:
      d = self.queue.get()
      if d is None:
        break
      self.bar.update(d)


class SqliteSet:

  def __init__(self, path):
    self.conn = sqlite3.connect(path)
    self.cursor = self.conn.cursor()

  def close(self):
    self.conn.close()

  def __contains__(self, m):
    sql = f"SELECT * FROM case_id WHERE case_id=?"
    res = self.cursor.execute(sql, (m, ))
    return res.fetchone() is not None


def merge_two_texts(text1, text2):
  """合并两个案号一样的裁判文书。合并过程中尽量将缺失字符�补全。

  如果无法合并，返回原来的文书；如果能合并，返回合并的文书和None。
  调用此函数可以使用返回的第二个元素是否为None来判断是否合并成功。

  注意：
  - 只考虑每个缺失的字符是一个字的情况。
  - 如果能合并，合并文书的格式会更加接近第一个文书。
  """

  # 简单的预处理，将连续出现的�变成一个�
  mask_char = "�"
  mask_ptn = r"��+"
  text1 = re.sub(mask_ptn, mask_char, text1.strip())
  text2 = re.sub(mask_ptn, mask_char, text2.strip())

  # 如果有一个文书是空的我们直接返回另一个
  if not text1:
    return text2, None
  if not text2:
    return text1, None

  # 寻找下一个非空字符，跳过\n,\t和空格
  ignored_chars = {"\n", "\t", " "}

  def _next_pos(text, i):
    i += 1
    while i < len(text) and text[i] in ignored_chars:
      i += 1
    if i >= len(text):
      i = -1
    return i

  # 逐字合并
  i = j = 0
  merged_text = ""
  while i != -1 and j != -1:
    if text1[i] == text2[j]:
      merged_text += text1[i]
    else:
      if text1[i] != mask_char and text2[j] != mask_char:
        return text1, text2
      # 现在只考虑缺失一个字符的情况
      if text1[i] == mask_char:
        merged_text += text2[j]
      else:
        merged_text += text1[i]
    ni = _next_pos(text1, i)
    if ni > i + 1:
      # 将第一个文书跳过的空字符加回来，所以最终合并文书更贴近第一个文书的格式
      merged_text += text1[i + 1: ni]
    i = ni
    j = _next_pos(text2, j)

  if i != -1 or j != -1:
    return text1, text2

  return merged_text, None


def merge_texts(texts):
  ret = []
  while len(texts) > 1:
    text = texts[0]
    remaining_texts = []
    for other in texts[1:]:
      text, other = merge_two_texts(text, other)
      if other is not None:
        remaining_texts.append(other)
    texts = remaining_texts
    ret.append(text)
  ret.extend(texts)
  return ret


bd_remove_words = [
  "{C}", "PAGE", "www.macrodatas.cn"
]
bd_stopwords = [
  "关注", "微信", "搜索", "马克", "马 克", "来自", "来源", "更多", "百度"
]


def clean_bd_case_text(text):
  for word in bd_remove_words:
    text = text.replace(word, "")
  text = text.replace("\u3000", " ").strip()
  cand = [len(text)]
  for word in bd_stopwords:
    idx = text.rfind(word)
    if idx != -1 and len(text) - idx <= 20:
      cand.append(idx)
  return text[:min(cand)]


def unescape(text: str) -> str:
  if type(text) is not str:
    return text
  return parser.unescape(text).strip()


def convert_chinese_to_digits(text):
  if "×" in text:
    return "0"
  if "十" in text:
    if text == "十":
      return "10"
    if text[0] == "十":
      text = "一" + text[1:]
    elif text[-1] == "十":
      text = text[:-1] + "〇"
    else:
      text = text.replace("十", "")
    return convert_chinese_to_digits(text)
  
  chinese = list("〇ＯO○0０元一二三四五六七八九十")
  digits = [0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  digits = map(str, digits)
  chinese2digits = dict(zip(chinese, digits))
  ret = ""
  for c in text:
    ret += chinese2digits[c]
  return ret

def extract_time(case_id, case_text):
  # 先尝试从文书中提取年月日
  ptn1 = r"([一二三四五六七八九十〇ＯO○0０]{4})年?([元一二三四五六七八九十×]{1,2})月([一二三四五六七八九十×]{1,3})日?"
  times = re.findall(ptn1, case_text)
  if times:
    y, m, d = map(convert_chinese_to_digits, times[0])
    if len(m) == 1:
      m = f"0{m}"
    if len(d) == 1:
      d = f"0{d}"
    return f"{y}{m}{d}"
  
  # 然后尝试从案号提取年
  ptn2 = r"[(（](\d{4})[）)]"
  year = re.findall(ptn2, case_id)
  if year:
    year = year[0]
    return f"{year}0000"
  
  # 最后返回 20240101
  return "20240101"