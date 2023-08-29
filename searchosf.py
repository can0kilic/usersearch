import csv
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_user_orcid(user_id):
    try:
        url = f"https://osf.io/api/v1/settings/social/{user_id}/"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            orcid = data.get('orcid')
            return orcid
    except:
        return None

def get_private_projects(osf_id):
    url = f"https://osf.io/{osf_id}/"


    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        private_projects = soup.select_one('.col-sm-6 h2')
        if private_projects:
            ext_text = private_projects.text
            ext_text = ext_text.replace("activity points","@")
            ext_text = ext_text.replace("activity point","@")
            ext_text = ext_text.replace("projects,","@")
            ext_text = ext_text.replace("project,","@")
            ext_text = ext_text.replace("public","@")
            ext_text = ext_text.replace("\n","")

            ext_text = ext_text.split("@")
            data = []
            if len(ext_text) > 1:
                for i in ext_text:

                    j = i.strip()
                    # print(j)

                    if j != "" and j != " ":
                        try:
                            data.append(int(j))
                        except:
                            pass
            try:

                if len(data) == 3:

                    pvt = data[-2] - data[-1]
                    return pvt
                else:
                    return 0

            except:
                return 0

def retrieve_public_projects(url):
    all_projects = []

    while url:
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            projects = data.get('data', [])
            all_projects.extend(projects)
            next_page = data.get('links', {}).get('next', None)
            url = next_page

    return all_projects, len(all_projects)





def retrieve_user_info(name,orcid=None):

    if orcid:
        url = f'https://api.osf.io/v2/search/users/?q="{name}" "{orcid}"'
    
        # print(url)
        print(url)
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            users = data.get('data', [])

            if users:
                user = users[0]
                user_id = user.get('id', '')
                user_orcid = get_user_orcid(user_id)

                if user_orcid == orcid:
                    user_attributes = user.get('attributes', {})
                    public_projects_link = user.get('relationships', {}).get('nodes', {}).get('links', {}).get('related', {}).get('href', '')

                    public_project_info = []

                    if public_projects_link:
                        public_projects, total_projects = retrieve_public_projects(public_projects_link)
                        for project in public_projects:
                            if project['attributes']['category'] == 'project':
                                project_info = (project['attributes']['title'], project['attributes']['date_created'])
                                public_project_info.append(project_info)

                        private_projects_count = get_private_projects(user_id)

                    user_info = {
                        'no_of_public_projects': len(public_project_info),
                        'no_of_private_projects': private_projects_count,
                        'osf_id': user_id,
                        'public_projects': public_project_info
                    }

                    return user_info

        return None

    else:
        url = f'https://api.osf.io/v2/search/users/?q="{name}"'
        print(url)
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            users = data.get('data', [])

            if users:
                user = users[0]
                user_id = user.get('id', '')
                user_attributes = user.get('attributes', {})
                public_projects_link = user.get('relationships', {}).get('nodes', {}).get('links', {}).get('related', {}).get('href', '')

                public_project_info = []

                if public_projects_link:
                    public_projects, total_projects = retrieve_public_projects(public_projects_link)
                    for project in public_projects:
                        if project['attributes']['category'] == 'project':
                            project_info = (project['attributes']['title'], project['attributes']['date_created'])
                            public_project_info.append(project_info)

                    private_projects_count = get_private_projects(user_id)

                user_info = {
                    'no_of_public_projects': len(public_project_info),
                    'no_of_private_projects': private_projects_count,
                    'osf_id': user_id,
                    'public_projects': public_project_info
                }

                return user_info
        return None



### # example usage
##name = "Michael Schulte-Mecklenbeck"
##user_info = retrieve_user_info(name,"0000-0002-0406-8809")
##print(user_info)
