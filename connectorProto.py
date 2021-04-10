# import libraries
from support_funcs import *
from string import Template
import pandas as pd
import json
from tqdm import tqdm

# get all repos this account has access to read
repos = get_all_repos(headers)

# and convert to pandas df
df = pd.DataFrame.from_dict(repos)
# everything below is in Zulu timezone
df["CreatedAt"] = pd.to_datetime(df.createdAt)
df["pushedAt"] = pd.to_datetime(df.pushedAt)
df["updatedAt"] = pd.to_datetime(df.updatedAt)

# extract data packets for each repo and initialize repo class objects
repos = []
allRepos = df.to_dict(orient="records")
for repo_data in allRepos:
    temp = repository_object(repo_data)
    repos.append(temp)
#repos = repos[0:10]
# process each of the trees
for repo in tqdm(repos,ascii=True,desc='Processing repos: '):
    repo.process_tree(cur_path="/",tree_part="")

data = "\n".join(list(map(lambda x: "\n".join(list(map(str,x.tree))).replace("'",'"'),repos))).strip()

# replace/update contents of a file system
amai = API_Interface()
response = requests.post(ALATION_HOST + '/api/v1/bulk_metadata/file_upload/' + str(DSID) + '/',
                         data=data,  
                         headers=amai.headers,
                         verify=CERTIFICATE) if USING_CER_FILE == 'Y' else requests.post(
                         ALATION_HOST + '/api/v1/bulk_metadata/file_upload/' + str(DSID) + '/',
                         json=self.data)
# process jupyter notebooks
process_ipynb(repos)

# process python, R, SQL, and text files
process_txt_code_files(repos)