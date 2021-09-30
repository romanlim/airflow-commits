# -*- coding: utf-8 -*-
"""
Spyder Editor

"""
#### Import required libraries


import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import seaborn as sns

#### Set configuration variables

per_page  = 100        # set number of rows to return each api call
since     = ''         # To define start date
page_start = 1

last_x = 6             # for compute datetime period to lookback
today = datetime.now() # set the current datetime
top_x = 5              # set the number of author to return for question 2 

endpoint  = 'https://api.github.com'

auth_token = '<Gihub token>'
url = 'https://api.github.com/repos/apache/airflow/commits'
header    = {'Accept': 'application/vnd.github.v3+json', 'Authorization': 'token ' + auth_token}

    
#### Compute 'since' date using a baseline date, period and time dimension
### end_date -> datetime object as the baseline
### last -> period to look back in integer
### type -> look back in year, month or day
def set_start_date(end_date = today, last = 1, type = 'm'):
    return_value = ''
    if type.lower() == 'y':
        return_value = end_date - relativedelta(years=last)
    elif type.lower() == 'm':
        return_value = end_date - relativedelta(months=last)
    elif type.lower() == 'd':
        return_value = end_date - relativedelta(days=last)
    else:
        return_value = end_date 
    return return_value.isoformat()[:-7]


#### Call api with parameter passed
### url -> Endpoint url
### header -> header parameter
### parameters -> parameters to be passed into the API call
def call_api(url, header, parameters):
    response = requests.get(url, headers=header, params=parameters)
    if response.status_code == 200:
        if not response.json() is None:
            return response.json()
        else:
            return None
    else:
        return None
  
#### Recursive api call since there is a limitation of 100 records, the function will loop from page 1 to the last page
### since_dt -> Set 'since' date
### per_page -> Set 'per_page' to 100 records to be return at each call
### page -> Set the starting page number
### url -> Endpoint url
### header -> header parameter
### parameters -> parameters to be passed into the API call
def loop_api_call(since_dt, per_page, page, url, header):
    done = False
    df = pd.DataFrame()
    while done == False:
        parameters = {'since': str(since_dt), 'per_page' : per_page, 'page' : page}
        response = call_api(url, header, parameters)
        if not response is None and response:
            df = df.append(pd.json_normalize(response), ignore_index=True)
            page += 1
        else:
            done = True
            
    return df

#### Create the hour block grouping
### hour -> numeric character
def hour_group(hour):
    group_value = None
    if str(hour).isnumeric():
        if hour >= 0 and hour < 3:
            group_value = '00-03'
        elif hour >= 3 and hour < 6:
            group_value = '03-06'
        elif hour >= 6 and hour < 9:
            group_value = '06-09'
        elif hour >= 9 and hour < 12:
            group_value = '09-12'
        elif hour >= 12 and hour < 15:
            group_value = '12-15' 
        elif hour >= 15 and hour < 18:
            group_value = '15-18'
        elif hour >= 18 and hour < 21:
            group_value = '18-21'
        elif hour >= 21:
            group_value = '21-00'   
    return group_value

#### Set the start date which is 6 month earlier from
since = set_start_date(today, 6, 'm'),

#### Return as dataframe from the recursive api call
result_df = loop_api_call(since, per_page, page_start, url, header)

#### Q1: determine the top 5 committers ranked by count of commits and their number of commits
### Count the number 'sha' group by author and author id
top_five_committers = result_df.groupby(['author.login','author.id'])['sha'].count().reset_index().sort_values('sha', ascending = False)[:top_x]
top_five_committers.rename(columns = {'sha': 'count'}, inplace=True)
print(top_five_committers) 

#### Q2: For the ingested commits, determine the committer with the longest commit streak.
### Get min and max of commit date group by author and author id
longest_commit_streak = result_df.groupby(['author.login','author.id']).agg(earliest_commit_date = ('commit.committer.date', np.min), latest_commit_date = ('commit.committer.date', np.max))
longest_commit_streak["earliest_commit_date"] = pd.to_datetime(longest_commit_streak["earliest_commit_date"], format="%Y-%m-%d")
longest_commit_streak["latest_commit_date"] = pd.to_datetime(longest_commit_streak["latest_commit_date"], format="%Y-%m-%d")
longest_commit_streak['streak_in_days'] = (longest_commit_streak['latest_commit_date'] - longest_commit_streak['earliest_commit_date']).dt.days
longest_commit_streak.sort_values('streak_in_days', ascending = False, inplace = True)
print(longest_commit_streak) 

#### Q3: For the ingested commits, generate a heatmap of number of commits count by all users by day of the week and by 3 hour blocks.
### Get the week day and hour from committed date, count number of 'sha'
commit_df = result_df[['author.login','author.id', 'sha', 'commit.committer.date']].copy()
commit_df['commit_date'] = pd.to_datetime(commit_df["commit.committer.date"], format="%Y-%m-%d")
commit_df['commit_weekday'] = commit_df['commit_date'].dt.day_name()
commit_df['commit_hour'] = commit_df['commit_date'].dt.hour
commit_df['commit_hour_block'] = commit_df['commit_hour'].apply(lambda x: hour_group(x))

hm_df = commit_df.groupby(['commit_weekday','commit_hour_block']).agg(count = ('author.id', 'count')).reset_index()
heatmap = sns.heatmap(hm_df.pivot('commit_weekday', 'commit_hour_block', 'count'), annot=True)


