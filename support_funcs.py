from string import Template
from config import *
import requests, json, re
from time import sleep
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

base_upload_api = Template("""/api/v1/bulk_metadata/custom_fields/$ct/$otype?replace_values=$flag""")

class API_Interface():
    """A simple class to manage communication between Alation API and the application(s)"""

    def __init__(self):
        try:
            self.data = dict(refresh_token=API_REFRESH_TOKEN, user_id=API_USER_ID)
            response = requests.post(ALATION_HOST +'/integration/v1/createAPIAccessToken/', json=self.data)
            self.api_token = response.json()
            self.headers = {"TOKEN": self.api_token['api_access_token'],'Content-type':'application/json'}
        except Exception as e:
            return(e)

# define the header for git
headers = {"Authorization": "Bearer {}".format(APIKEY)}

### Query Parts

part1 = """{
repositoryOwner(login: "$owner") {
repository(name: "$repo") {"""
part2_root = """
  object(expression: "master:") {"""
part2_branch = """
  object(expression: "master:$path") {"""
part3 = """
  ... on Blob {
    text
    byteSize
    isTruncated
    isBinary
  }
  ... on Tree{
    entries{
      name
      type
      mode
    }
  }
}
}
}
}
"""

start = """{viewer {""" 
middle_first = """repositories(first: $num) {"""
middle_not_first = """repositories(first: $num, after: \"$cur\") {"""
end = """    pageInfo {
    hasNextPage
    endCursor
    }
edges {
    node {
        id
        name
        description
        isPrivate
        diskUsage
        createdAt
        forkCount
        sshUrl
        url
        isArchived
        isFork
        isLocked
        lockReason
        isMirror
        pushedAt
        updatedAt
        owner {
           login
           id
           }
        languages(first: 100) {
            edges {
                node {
                    name
                    }
                }
            }
        }
    }
}
}}"""
###

# The query below returns first 30 repos. The result comes back with 
# page info will return: {"pageInfo": {"hasNextPage": False,
#     "endCursor": "Y3Vyc29yOnYyOpHOCZREog=="}
# which will indicate if there is a next page or not.
def get_repos(n=30,first_query=True,end_cursor=""):
    """n = number of repos to get\nfirstQuery = if True"""

    # case where cursors need to be handled
    if first_query:
        queryTemplate = Template(start + middle_first + end)
        query = queryTemplate.substitute(num=n)
    else:
        queryTemplate = Template(start + middle_not_first + end)
        query = queryTemplate.substitute(num=n,cur=end_cursor)
    
    return(query)

# The following function returns a list of all repositories with certain
# metadata attributes
def get_all_repos(headers,batch_size=30):
    """A function to extract metadata on all repos, batch_size repos at a time."""
    # initialize
    repos = []
    first_query = True
    has_next_page = True
    end_cursor = ""

    while has_next_page:
        # get query to pull for initial batch_size repos
        query = get_repos(n=batch_size,first_query=first_query,end_cursor=end_cursor)
        first_query = False
        # get result
        result = run_query(query=query,headers=headers)
        # check if there are more pages
        has_next_page = result["data"]["viewer"]["repositories"]["pageInfo"].get("hasNextPage",False)
        # if so, then get the endCursor
        if has_next_page:
            end_cursor = result["data"]["viewer"]["repositories"]["pageInfo"]["endCursor"]
        # repos to process
        incoming_repo_list = result["data"]["viewer"]["repositories"]["edges"]
        # flatten the data
        for repo in incoming_repo_list:
            temp = repo["node"]
            temp["languages"] = language_parser(temp)
            temp["owner_id"] = temp["owner"]["id"]
            temp["owner"] = temp["owner"]["login"]
            repos.append(temp)
            
    return(repos)

# function to run the queries
def run_query(query,headers):
    request = requests.post("https://api.github.com/graphql", json={"query": query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))
        
def language_parser(data):
    """Function to parse out a pipe delimited list of first 100 languages attached to a repo."""
    out_lang_list = []
    lang_list = data["languages"]["edges"]
    for lang in lang_list:
        out_lang_list.append(lang["node"]["name"])
    
    out_lang_list = "|".join(out_lang_list)
    return(out_lang_list)

# the following function returns either the query for tree roots or tree
# branch depending on what "part" is asked for
def get_tree_parts(owner,repo,part):
    """owner: repo owner, repo: repo name, part: root or path you wish to query"""

    """To get files at a path in your repo, the part2_branch object expression must be:
    expression: "master:<path in repo>"""
    
    if part == "root":
        # assemble the right parts
        query = part1 + part2_root + part3
        # create the query
        query_template = Template(query)
        # return after substitution
        return(query_template.substitute(owner=owner,repo=repo))
    else:
        # assemble the right parts
        query = part1 + part2_branch + part3
        # create the query
        query_template = Template(query)
        # return after substitution
        return(query_template.substitute(owner=owner,repo=repo,path=part))


# create a class to hold individual repository objects
# we initialize it with data collected before
class repository_object:
    """Repo class to hold repository objects obtained previously"""
    
    def __init__(self,data):
        """initialization is done with a dictionary data packet"""
        # pass in the previously collected properties
        # the data must be a dictionary of key-value pairs
        self.properties = data
        self.tree = []
        self.blob_tree = []

    def get_attr(self,attribute):
        """Simple function to get some repo attribute"""
        try:
            return(self.properties[attribute])
        except KeyError as error:
            print("The given key {} does not exists.".format(error))
            print("Valid attribute names: {}".format(", ".join(self.properties.keys())))

    
    def extend_repo_tree(self,path,name,is_directory,size_in_bytes=4000,f_text=""):
        """A function to extend the file structure tree"""

        temp = {"path": path, "name": name, "is_directory": is_directory,
                "owner": self.properties["owner"], "size_in_bytes": size_in_bytes,
                "ts_last_accessed": str(self.properties["updatedAt"].strftime("%Y-%m-%d %H:%M:%S")),
                "ts_last_modified": str(self.properties["updatedAt"].strftime("%Y-%m-%d %H:%M:%S"))}

        self.tree.append(temp)

        if len(f_text) > 0:
            temp = {"path": path, "name": name, "is_directory": is_directory,
                    "owner": self.properties["owner"], "size_in_bytes": size_in_bytes,
                    "ts_last_accessed": str(self.properties["updatedAt"].strftime("%Y-%m-%d %H:%M:%S")),
                    "ts_last_modified": str(self.properties["updatedAt"].strftime("%Y-%m-%d %H:%M:%S")),
                    "f_text":f_text}
            
            self.blob_tree.append(temp)
        

    def process_blob(self,entry,cur_path,tree_part):
        """A function to process file entries in the repo"""

        repo_name = self.get_attr("name")
        repo_owner = self.get_attr("owner")

        # build information extraction query
        if tree_part == "":
            t_query = get_tree_parts(repo=repo_name,owner=repo_owner,part=entry["name"])
        else:
            t_query = get_tree_parts(repo=repo_name,owner=repo_owner,part=tree_part + "/" + entry["name"])

        res = run_query(t_query,headers)

        # extract file size
        byteSize = res["data"]["repositoryOwner"]["repository"]["object"]["byteSize"]

        # extract file content
        if res["data"]["repositoryOwner"]["repository"]["object"]['text']:
            f_text = res["data"]["repositoryOwner"]["repository"]["object"]["text"]
        else:
            f_text = ""

        # collect information in the entry
        e_name = entry["name"]

        self.extend_repo_tree(path=cur_path,is_directory="false",name=e_name,size_in_bytes=byteSize,f_text=f_text.encode("UTF-8"))

    def process_tree(self,cur_path,tree_part):
        """This recursive fuctions crawls the repo tree and extract information"""
        
        repo_name = self.get_attr("name")
        repo_owner = self.get_attr("owner")

        # get the current level of branchs
        if cur_path == "/":
            # record the current location on the tree
            self.extend_repo_tree(path=cur_path,is_directory="true",name=repo_name)
            cur_path = cur_path + repo_name

        branch_q = get_tree_parts(repo=repo_name,owner=repo_owner,part=tree_part)
        repo_branch = run_query(branch_q,headers)

        # process entries if the return is not empty
        if repo_branch["data"]["repositoryOwner"]["repository"]["object"] is not None:
            entries = repo_branch["data"]["repositoryOwner"]["repository"]["object"]["entries"]

            for entry in entries:
                # collect information in the entry
                e_name = entry["name"]
                e_type = entry["type"]
                e_path = cur_path

                # if it is a folder, then we need to dig further in
                if entry["type"] == "tree":

                    self.extend_repo_tree(path=cur_path,is_directory="true",name=e_name)

                    if tree_part == "":
                        self.process_tree(cur_path = cur_path + "/" + e_name, tree_part = e_name)
                    else:
                        self.process_tree(cur_path = cur_path + "/" + e_name, tree_part = tree_part + "/" + e_name)

                # if it is a file, then we need to get process the file
                elif entry["type"] == "blob":
                    # process the file entry
                    self.process_blob(entry = entry, cur_path = cur_path, tree_part = tree_part)


# Table template for result set
html_tb_template = Template("""<table style="width: 924px;">
    <thead>
        <tr>
            <th>Cell Number</th>
            <th>Jupyter Cell</th>
        </tr>
    </thead>
    <colgroup>
        <col style="width: 56px;">
            <col style="width: 818px;">
    </colgroup>
    <tbody>
        $rows
    </tbody>
</table>
""")

# row template for result set
html_row_template = Template("""<tr>
            <td>$cell_num</td>
            <td>$row</td>
        </tr>""")

# row template for ipynb cell
ipynb_row = Template("""<tr>
            <td></td>
            <td><pre>$cmd</pre></td>
        </tr>
""")

def process_ipynb(repos):
    blob_data = []
    # now time to process the blobs
    for repo in repos:
        for blob in repo.blob_tree:
            f_key = str(DSID) + blob["path"] + "/" + blob['name']
            f_text = blob["f_text"]
            if blob['name'].split(".")[-1] == "ipynb":
                blob_data.append({"key":f_key,"f_text":f_text})

    # iterate through each notebook and extract the information
    for temp_nb in blob_data:
        try:
            nb_text = json.loads(temp_nb['f_text'].decode())

            # extract simple metadata
            n_cells = len(nb_text['cells'])
            py_version = nb_text['metadata']['language_info']['version']

            # process each cell
            cell_rows = []
            for i in range(0,n_cells):
                cell = nb_text['cells'][i]
                cell_source_data = []
                # grab each entry 
                for cell_entry in cell['source']:
                    cell_source_data.append(cell_entry)
                # create cell code entry
                cell_source = ipynb_row.substitute(cmd = ''.join(cell_source_data))
                cell_number = i + 1
                cell_rows.append({'cell_num':cell_number,'row':cell_source})

            filled_tb_rows = '\n'.join(list(map(lambda x: html_row_template.substitute(cell_num=x['cell_num'],row=x['row']),cell_rows)))
            out_data = html_tb_template.substitute(rows=filled_tb_rows)

            temp_nb['description'] = 'Python version: {}'.format(py_version) + '\n' +'Number of Cells: {}'.format(str(n_cells)) + '\n' + out_data
        except:
            pass

    upload_data = list(map(lambda x: {'key':x.get('key',-1),'description':x.get('description',0)},blob_data))

    data = "\n".join(list(map(lambda x: json.dumps(x),upload_data)))

    # harden this API call
    amai = API_Interface()
    response = requests.post(ALATION_HOST + "/api/v1/bulk_metadata/custom_fields/default/filesystem?replace_values=true",
                             data=data, headers=amai.headers)

def process_txt_code_files(repos):
    blob_data = []
    # now time to process the blobs
    for repo in repos:
        for blob in repo.blob_tree:
            f_key = str(DSID) + blob["path"] + "/" + blob['name']
            f_text = blob["f_text"]
            if blob['name'].split(".")[-1].lower() in ['py','r','sql','txt','md','c','cpp','xml']:
                blob_data.append({"key":f_key,"description":ipynb_row.substitute(cmd = f_text.decode())})

    data = "\n".join(list(map(lambda x: json.dumps(x),blob_data)))

    # Harden this API call
    amai = API_Interface()
    response = requests.post(ALATION_HOST + "/api/v1/bulk_metadata/custom_fields/default/filesystem?replace_values=true",
                                 data=data, headers=amai.headers)