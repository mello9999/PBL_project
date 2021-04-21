import pandas as pd         
import os                                                                                                              
from header import *

# transform
sa_01 = pd.read_csv('../../data/01_Subject_attributes.csv', header=None)
sa_01.columns = sa_01_header
sa_01 = sa_01[sa_01['SubjectID'].notna()].reset_index(drop=True)


li_02 = pd.read_csv('../../data/02_Life_independence.csv', header=None)
li_02.columns = li_02_header
li_02 = li_02[li_02['SubjectID'].notna()].reset_index(drop=True)

ir_04 = pd.read_csv('../../data/04_Inspection results.csv', header=None)
ir_04.columns = ir_04_header
ir_04 = ir_04[ir_04['SubjectID'].notna()].reset_index(drop=True)


ev_05 = pd.read_csv('../../data/05_evaluation.csv', header=None)
ev_05.columns = ev_05_header
ev_05 = ev_05[ev_05['SubjectID'].notna()].reset_index(drop=True)


# transform sensing data
f = []
for (dirpath, dirnames, filenames) in os.walk('../../data/71_sensing_data'):
  n = []
  for i in filenames:
    if i[-3:] != 'csv':
      continue
    n.append(os.path.join(dirpath , i))
  f.extend(n)

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
s = set()
for i in f:
  c = i[45:]
  a = find_nth(c, '_', 1)
  b = find_nth(c, '_', 2)
  s.add(c[a+1:b])

for d in s:
  exec(d + " = pd.DataFrame()")

for file in f:
  c = file[45:]
  a = find_nth(c, '_', 1)
  b = find_nth(c[a+1:], '_', 1)
  if c[a+1:a+b+1] == 'activ':
    tem = pd.read_csv(file)
    tem['SubjectID'] = c[:a]
    activ = activ.append(tem, ignore_index=True)
  if c[a+1:a+b+1] == 'exist':
    tem = pd.read_csv(file)
    tem['SubjectID'] = c[:a]
    exist = exist.append(tem, ignore_index=True)
  if c[a+1:a+b+1] == 'sphy':
    tem = pd.read_csv(file)
    tem['SubjectID'] = c[:a]
    sphy = sphy.append(tem, ignore_index=True)
  if c[a+1:a+b+1] == 'temp':
    tem = pd.read_csv(file)
    tem['SubjectID'] = c[:a]
    temp = temp.append(tem, ignore_index=True)
  if c[a+1:a+b+1] == 'weight':
    tem = pd.read_csv(file)
    tem['SubjectID'] = c[:a]
    weight = weight.append(tem, ignore_index=True)
    
activ['SubjectID'] = activ['SubjectID'] + '01'
exist['SubjectID'] = exist['SubjectID'] + '01'
sphy['SubjectID'] = sphy['SubjectID'] + '01'
temp['SubjectID'] = temp['SubjectID'] + '01'
weight['SubjectID'] = weight['SubjectID'] + '01'    


be_li_02 = li_02[['SubjectID', 'Appetite Questionnaire results', 'Preference Questionnaire results', 'Sleep Questionnaire results', 'Anxiety about health Questionnaire results', 
                  'I can go up and down stairs without being transmitted to the railing or wall','I can walk for more than 15 minutes', 'I am actively going out',
                  'Do the cleaning and washing yourself', 'Shop for daily necessities yourself', 'No weight loss of more than 2-3 kg in the last 6 months', 
                  'No weight gain of more than 2-3 kg in the last 2 months']]
be_ev_05 = ev_05[['SubjectID', 'Exercise function', 'Nutrition']]
behavior = pd.merge(be_li_02, be_ev_05, on='SubjectID', how='outer')

medicine_info = sa_01[['SubjectID', 'Antihypertensive', 'Antidepressant', 'Osteoporosis drug', 'Antidiabetic drug']]

ihi_ir_04 = ir_04[['SubjectID', 'Inspection date', 'Total cholesterol', 'LDL cholesterol', 
                              'HDL cholesterol', 'HbA1c', 'AST', 'ALT', 'LDH', 
                              'Na', 'K', 'ECG abnormalities', 
                              'History of coronary artery disease', ]]
ihi_weight = weight[['SubjectID', ' Measure Date Time', ' Body Fat Percentage', ' Basal Metabolism']]
internal_health_info = pd.merge(ihi_weight, ihi_ir_04, on='SubjectID', how='left')

bi_sphy= sphy[['SubjectID', ' Measure Date Time', ' Systolic Pressure', ' Diastolic Pressure', 
                   ' Mean Arterial Pressure',' Pulse Rate', 
                   ' Irregular Pulse Flag',' Pulse Rate Range Detection Flag']]
bi_ir_04 = ir_04[['SubjectID', 'Blood sugar']]
blood_info = pd.merge(bi_sphy, bi_ir_04, on='SubjectID', how='left')

disease_info = ev_05[['SubjectID', 'Arrhythmia', 'Diabetes mellitus']]

pe_sa_01 = sa_01[['SubjectID', 'Age', 'Gender', 'Height', 'Body weight', 'Smoking', 'Drinking', ]]
pe_weight = weight[['SubjectID', ' Measure Date Time', ' BMI', ' Body Age']]
person = pd.merge(pe_weight, pe_sa_01, on='SubjectID', how='left')

sl_activ = activ[['SubjectID', ' Measure Date Time', ' Sleep Hour', ' Sleep Minute']]
sl_li_02 = li_02[['SubjectID', 'Sleep Questionnaire results']]
sleep_info = pd.merge(sl_activ, sl_li_02, on='SubjectID', how='left')

time_info = activ[[' Measure Date Time']]
time_info = time_info.append([exist[[' Measure Date Time']], sphy[[' Measure Date Time']], \
            sleep_info[[' Measure Date Time']], person[[' Measure Date Time']], internal_health_info[[' Measure Date Time']], \
            blood_info[[' Measure Date Time']]])
time_info = time_info.drop_duplicates(subset=[' Measure Date Time']).reset_index(drop=True)
time_info['year'] = pd.to_datetime(time_info[' Measure Date Time']).dt.year
time_info['month'] = pd.to_datetime(time_info[' Measure Date Time']).dt.month
time_info['day'] = pd.to_datetime(time_info[' Measure Date Time']).dt.day
time_info['week'] = pd.to_datetime(time_info[' Measure Date Time']).dt.week
time_info['weekday'] = pd.to_datetime(time_info[' Measure Date Time']).dt.weekday
time_info['hour'] = pd.to_datetime(time_info[' Measure Date Time']).dt.hour
time_info['minute'] = pd.to_datetime(time_info[' Measure Date Time']).dt.minute
time_info['second'] = pd.to_datetime(time_info[' Measure Date Time']).dt.second

behavior.to_csv('../../data/star_schema/behavior.csv', index=False)
sleep_info.to_csv('../../data/star_schema/sleep_info.csv', index=False)
medicine_info.to_csv('../../data/star_schema/medicine_info.csv', index=False)
internal_health_info.to_csv('../../data/star_schema/internal_health_info.csv', index=False)
blood_info.to_csv('../../data/star_schema/blood_info.csv', index=False)
disease_info.to_csv('../../data/star_schema/disease_info.csv', index=False)
time_info.to_csv('../../data/star_schema/time_info.csv', index=False)
person.to_csv('../../data/star_schema/person.csv', index=False)
