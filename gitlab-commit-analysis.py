# -*- coding: utf-8 -*
 
from ctypes import util
from functools import singledispatch

import gitlab
import json
import re
import sys
from dateutil import parser
from datetime import datetime, date, timedelta
# from sqlalchemy import null
import traceback
 
def mysql_insert(*data):
    import pymysql

    # Connect to the database
    connection = pymysql.connect(host='',
                                 user='',
                                 password='',
                                 database='',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            print(data)
            sql = "INSERT INTO `llp_git` (`repo`, `dt`,`email`,`pdate`,`commitid`,`committitle`,`commitmsg`,`codea`,`codeb`) VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s)"
            cursor.execute(sql, data)
            connection.commit()
 
try:

    since = sys.argv[1]
    end = sys.argv[2]
except Exception as e:
    since = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%dT00:00:00Z")
    end = date.today().strftime("%Y-%m-%dT00:00:00Z")

# private token or personal token authentication (self-hosted GitLab instance)
gl = gitlab.Gitlab(url='http://localhost:8765/',
                   private_token='')

# 获取所有仓库
# projects = gl.projects.list(all=True,page=1, per_page=150)
# for project in projects:
#    print(project.id ,project.path_with_namespace)

# 仓库名字
projectid = ["仓库1", "仓库2"]
userCommitd = {}
userCommitDayd = {}
repoCommitd = {}  # 仓库提交总次数
repoCommitDayd = {}  # 仓库提交时间维度总次数

for project_name_with_namespace in projectid:

    repoCommitd[project_name_with_namespace] = 0
 
    try:
        project = gl.projects.get(project_name_with_namespace)
    except Exception as e:
        print("Error: ", e)
        traceback.print_exc()
        continue

    # 获取某个仓库非merge、tag等commit
    try:
        # commits = project.commits.list(  all=True,page=1, per_page=100,since='2022-06-14T00:00:00Z' ,as_list =True )
        commits = project.commits.list(
            all=True, page=1, per_page=100, since=since, util=end, as_list=True)
    except Exception as e:
        try:
            commits = project.commits.list(
                all=True, page=1, per_page=100, since=since, util=end, as_list=True)
        except Exception as e:
            traceback.print_exc()
            print("Error: ", e)
            continue

    k = re.compile(r'Merge|合并')
    for co in commits:

        # print(co.merge_requests() )
        # print(co.refs('branch'))
        # print(co.short_id,co.title,co.created_at,co.committed_date,str.isalnum(co.message),co.author_name,co.author_email,sep="=>")

        rs = re.findall(k, co.title)
        if rs :
          continue

        branch = co.refs('branch')[0]

        # print(branch["type"],branch["name"], co.short_id,co.title,co.created_at,co.committed_date,str.isalnum(co.message),co.author_name,co.author_email,sep="=>")

        commitid = co.id  # commitid
        committitle = co.title
        commitmsg = str.strip(co.message)
        dt = str(datetime.strptime(co.committed_date,
                                   '%Y-%m-%dT%H:%M:%S.000+08:00'))

        codeadditions = 0
        codedeletions = 0
        try:
            signlecommit = project.commits.get(commitid)
            print(signlecommit.stats)
            codeadditions = signlecommit.stats["additions"]
            codedeletions = signlecommit.stats["deletions"]
        except Exception as e:
            continue

        # 更新到数据库中
        mysql_insert(project_name_with_namespace, dt, co.author_email,
                     dt[:10], commitid, committitle, commitmsg, codeadditions, codedeletions)

        """
        # 提交者总次数维度
        if co.author_email in userCommitd : 
            userCommitd[co.author_email] +=1 
        else:
            userCommitd[co.author_email] = 1 
        
        # 提交者时间维度提交次数
        day = parser.parse(co.committed_date)
        dayr = str(day.year) + str(day.month) + str(day.day)

        if co.author_email not in userCommitDayd :
            userCommitDayd[co.author_email] = {}
            userCommitDayd[co.author_email][dayr] = 1 
        else :
            if dayr not in userCommitDayd[co.author_email] :
                userCommitDayd[co.author_email][dayr] = 1
            else :
                userCommitDayd[co.author_email][dayr] += 1
        
        # 仓库维度统计次数
        repoCommitd[project_name_with_namespace] +=1 
        """

# gitlab.exceptions.GitlabHttpError: 404: 404 Project Not Found

# print(userCommitd)
# print(userCommitDayd)
# print(repoCommitd)
