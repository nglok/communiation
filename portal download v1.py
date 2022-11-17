#!/usr/bin/env python
# coding: utf-8

# In[1]:

import time
import pandas as pd
import requests
from datetime import datetime
import os
import zipfile

def lookup (df, lookup_col, reply_col, lookup_value):
    return df[reply_col][df[lookup_col]==lookup_value].iloc[0]

#environment
base_folder = os.getcwd()
settings_df = pd.read_excel(base_folder+"\\settings sources.xlsx", sheet_name=None)
df = settings_df["paths"]
destination_folder = lookup(df,"to","path","destination folder")
ssl_dev = lookup(df,"to","path","ssl")

#initialize settings
api_info = settings_df["links"]
login_info = settings_df["login info"]

#load api links from file
for api in api_info['Title']:
    command = f"{api}_link = api_info[api_info['Title']=='{api}']['url'].iloc[0]"
    exec(command)

#load login info from file
for info in ["username", "password"]:
    command = f"{info} = login_info.iloc[:,1][login_info.iloc[:,0]=='{info}'].iloc[0]"
    exec(command)

login_password = {"username":username,"password":password}


# In[2]:

#establish session

s = requests.session()

#request1 log in
r1 = s.post(login_link, data = login_password, verify = ssl_dev)

if (r1.status_code == 200):
    print("logged in")

a_key = r1.json()['result']                   #key given to identified user
cook_key = r1.headers['set-cookie']           #session cookie

#create header template from file and response1
headers_df = settings_df["headers"]
headers_temp = {}
for i in headers_df.index:
    headers_temp[headers_df.iloc[i]["attribute"]] = headers_df.iloc[i]["value"]
headers_temp["authorization"] = a_key
headers_temp["cookie"] = cook_key
headers_temp["content-type"] = "application/json"


# In[3]:

#request2 findn out batch id
headers = headers_temp
headers["content-type"] = "application/json"

setting= '{"pageNum":1,"pageSize":1000000}'

r2 = s.post(ETL_status_link, data = setting, headers = headers, verify = ssl_dev)

if r2.status_code == 200:
    print("fetched batch statuses")

batch_information = pd.DataFrame(r2.json()['result'])

latest_batchId = batch_information['batchId'][batch_information["latestProcessStatus"]!="Running"].iloc[-1]


# In[4]:


#request 3 find out file id

payload = '{'+f'"sortField":"uploadTime","sortOrder":"desc","batch_id":"{latest_batchId}"'+"}"

r3 = s.post(getEtlOutputFilelist_link, data = payload, headers = headers, verify=ssl_dev)

if r3.status_code == 200:
    print("extracted file_list")

complete_filelist = pd.DataFrame(r3.json()['result'])      #file id, name, file type, file type code are stored in a DataFrame

filetypes = complete_filelist[["fileType","fileTypeCode"]].drop_duplicates()
filetypecodes = filetypes["fileTypeCode"].unique()

downloadjobs = {}
filetype_dict = {}

for fileTypeCode in filetypecodes:
    downloadjobs[fileTypeCode] = complete_filelist["id"][complete_filelist["fileTypeCode"]==fileTypeCode]
    filetype_dict[fileTypeCode] = filetypes["fileType"][filetypes["fileTypeCode"]==fileTypeCode].iloc[0]


# In[5]:


# folder creation

if not os.path.exists(destination_folder):
    os.mkdir(destination_folder)
    print(f"created folder:\t{destination_folder}")

folder_dict = {}
for fileTypeCode in downloadjobs.keys():
    fileType = filetypes["fileType"][filetypes["fileTypeCode"]==fileTypeCode].iloc[0]
    folder_dict[fileTypeCode] = fileType
    subfolder = destination_folder+"\\"+fileType
    if not os.path.exists(subfolder):
        os.mkdir(subfolder)
        print(f"created folder:\t{subfolder}")


#request 4 download

#for fileTypeCode in downloadjobs.keys():

for fileTypeCode in downloadjobs.keys():
    t = time.time()
    setting = f'_t=&id={",".join(downloadjobs[fileTypeCode])}'
    r4 = s.get(downloadFile_link, params = setting, headers = headers, verify=ssl_dev)
    folderpath = destination_folder+f"\\{folder_dict[fileTypeCode]}"
    filepath = folderpath+f"\\{latest_batchId}_{fileTypeCode}.zip"
    file = open(filepath,"wb")
    file.write(r4.content)
    file.close()
    zipfile.ZipFile(filepath).extractall(folderpath)
    print(f"{fileTypeCode} - downloaded and decompressed",(t-time.time())*-1, sep = "\t")
