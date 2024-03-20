from huggingface_hub import HfApi, CommitOperationAdd
import utils

REPO_ID = "wormtooth/MNBVC-judgment"

def get_uploaded_files(api):
  ret = list(api.list_repo_tree(REPO_ID, path_in_repo="data", repo_type="dataset"))
  files_set = set()
  for path in ret:
    files_set.add(path.path.split("/")[1])
  return files_set

def upload_files(api, paths):
  operations = []
  for path in paths:
    op = CommitOperationAdd(path_in_repo=f"data/{path.name}", path_or_fileobj=path)
    operations.append(op)
  msg = f"Upload {len(paths)} files"
  api.create_commit(REPO_ID, operations, repo_type="dataset", commit_message=msg)

if __name__ == "__main__":
  api = HfApi()
  uploaded_files_set = get_uploaded_files(api)

  result_folder = utils.get_results_path() / "data"
  gz_file_paths = [
    path
    for path in result_folder.glob("*.jsonl.gz")
    if path.name not in uploaded_files_set
  ]
  
  batch_size = 20
  for i in range(0, len(gz_file_paths), batch_size):
    paths = gz_file_paths[i: i + batch_size]
    upload_files(api, paths)

    