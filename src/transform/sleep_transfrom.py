import pandas as pd
import datetime

sleep = pd.read_csv('../../data/star_schema/sleep_info.csv')
time = pd.read_csv('../../data/star_schema/time_info.csv')

sleep_time = pd.merge(sleep, time, how="left", on=[' Measure Date Time'])

def dt(x):
  tail = x.tail(1)[' Measure Date Time'].values[0]
  head = x.head(1)[' Measure Date Time'].values[0]
  
  div = datetime.datetime.strptime( tail, ' %Y-%m-%dT%H:%M:%S') - datetime.datetime.strptime( head, ' %Y-%m-%dT%H:%M:%S')
  seconds = div.days * 24 * 3600 + div.seconds 
  minutes, seconds = divmod(seconds, 60)
  hours, minutes = divmod(minutes, 60)
  days, hours = divmod(hours, 24)
  return hours + minutes/60

real_sleep = sleep_time['Sleep Questionnaire results']=="Sleeping "
sleep_div = sleep_time[['SubjectID', 'year', 'month', 'day',  ' Measure Date Time']][real_sleep].groupby(['SubjectID', 'year', 'month', 'day']).apply(dt)
sleep_div.to_csv('../../data/star_schema/sleep_div.csv')