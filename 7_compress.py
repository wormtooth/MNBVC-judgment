import gzip

from tqdm import tqdm

import utils


def get_all_resulted_gzips():
  path = utils.get_results_path()
  paths = list(path.glob("*/*.jsonl"))
  return paths

def get_final_result_folder():
  path = utils.get_results_path()
  path = path / "final"
  if not path.exists():
    path.mkdir()
  return path

if __name__ == "__main__":
  paths = get_all_resulted_gzips()
  result_folder = get_final_result_folder()
  for idx, path in tqdm(enumerate(paths)):
    gzip_path = result_folder / f"{idx:03d}.jsonl.gz"
    with open(path, "rb") as fin, gzip.open(gzip_path, "wb") as fout:
      fout.writelines(fin)