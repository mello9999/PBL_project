import math
import numpy as np
import pandas as pd 

from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.covariance import EllipticEnvelope

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

import matplotlib.pyplot as plt
import plotly.express as px


### Read data

behavior = pd.read_csv('../../data/star_schema/behavior.csv')
blood_info = pd.read_csv('../../data/star_schema/blood_info.csv')
disease_info = pd.read_csv('../../data/star_schema/disease_info.csv')
internal_health_info = pd.read_csv('../../data/star_schema/internal_health_info.csv')
medicine_info = pd.read_csv('../../data/star_schema/medicine_info.csv')
person = pd.read_csv('../../data/star_schema/person.csv')
sleep_info = pd.read_csv('../../data/star_schema/sleep_info.csv')
time_info = pd.read_csv('../../data/star_schema/time_info.csv')

##############


### Join tables

star_1 = pd.merge(person, time_info, on=' Measure Date Time', how='left')
sleep_info_time = pd.merge(sleep_info, time_info, on=' Measure Date Time', how='left')[[
                          'SubjectID',' Sleep Hour', ' Sleep Minute',
                          'year', 'month', 'day', 'hour']]
sleep_info_time = sleep_info_time.drop_duplicates(['SubjectID', 'year', 'month', 'day', 'hour']).reset_index(drop=True)
star_2 = pd.merge(star_1, sleep_info_time, on=['SubjectID', 'year',	'month', 'day', 'hour'],  how='left')
star_3 = pd.merge(star_2, medicine_info, on=['SubjectID'],  how='left')
internal_health_info_time = pd.merge(internal_health_info, time_info, on=' Measure Date Time', how='left')[[
                          'SubjectID',' Body Fat Percentage', ' Basal Metabolism',
                          'Inspection date', 'Total cholesterol', 'LDL cholesterol',
                          'HDL cholesterol', 'HbA1c',	'AST', 'ALT',	'LDH', 'Na', 'K',
                          'year', 'month', 'day', 'hour']]
internal_health_info_time = internal_health_info_time.drop_duplicates(['SubjectID', 'year', 'month', 'day', 'hour']).reset_index(drop=True)
star_4 = pd.merge(star_3, internal_health_info_time, on=['SubjectID', 'year',	'month', 'day', 'hour'],  how='left')
star_5 = pd.merge(star_4, disease_info, on=['SubjectID'],  how='left') 
blood_info_time = pd.merge(blood_info, time_info, on=' Measure Date Time', how='left')[[
                          'SubjectID',' Systolic Pressure', ' Diastolic Pressure',
                          ' Mean Arterial Pressure', ' Pulse Rate', ' Irregular Pulse Flag',
                          ' Pulse Rate Range Detection Flag', 'Blood sugar',
                          'year', 'month', 'day', 'hour']]
blood_info_time = blood_info_time.drop_duplicates(['SubjectID', 'year', 'month', 'day', 'hour']).reset_index(drop=True)
star_6 = pd.merge(star_5, blood_info_time, on=['SubjectID', 'year',	'month', 'day', 'hour'],  how='left')
star_data = pd.merge(star_6, behavior, on=['SubjectID'],  how='left') 

##############

### Dummies

star_data_dummied = pd.concat([star_data, pd.get_dummies(star_data['Gender'], prefix='Gender')], axis=1)
star_data_dummied = star_data_dummied.drop(['Gender'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Drinking'], prefix='Drinking')], axis=1)
star_data_dummied = star_data_dummied.drop(['Drinking'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Antihypertensive'], prefix='Antihypertensive')], axis=1)
star_data_dummied = star_data_dummied.drop(['Antihypertensive'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Osteoporosis drug'], prefix='Osteoporosis')], axis=1)
star_data_dummied = star_data_dummied.drop(['Osteoporosis drug'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Antidiabetic drug'], prefix='Antidiabetic')], axis=1)
star_data_dummied = star_data_dummied.drop(['Antidiabetic drug'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Diabetes mellitus'], prefix='Diabetes')], axis=1)
star_data_dummied = star_data_dummied.drop(['Diabetes mellitus'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data[' Irregular Pulse Flag'], prefix='Irregular_Pulse_Flag')], axis=1)
star_data_dummied = star_data_dummied.drop([' Irregular Pulse Flag'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Appetite Questionnaire results'], prefix='Appetite')], axis=1)
star_data_dummied = star_data_dummied.drop(['Appetite Questionnaire results'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Preference Questionnaire results'], prefix='Preference')], axis=1)
star_data_dummied = star_data_dummied.drop(['Preference Questionnaire results'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Sleep Questionnaire results'], prefix='Sleep_Quest')], axis=1)
star_data_dummied = star_data_dummied.drop(['Sleep Questionnaire results'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Anxiety about health Questionnaire results'], prefix='Anxiety')], axis=1)
star_data_dummied = star_data_dummied.drop(['Anxiety about health Questionnaire results'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['I can go up and down stairs without being transmitted to the railing or wall'], prefix='up/down stairs')], axis=1)
star_data_dummied = star_data_dummied.drop(['I can go up and down stairs without being transmitted to the railing or wall'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['I can walk for more than 15 minutes'], prefix='walk')], axis=1)
star_data_dummied = star_data_dummied.drop(['I can walk for more than 15 minutes'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['I am actively going out'], prefix='going_out')], axis=1)
star_data_dummied = star_data_dummied.drop(['I am actively going out'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Do the cleaning and washing yourself'], prefix='cleaning')], axis=1)
star_data_dummied = star_data_dummied.drop(['Do the cleaning and washing yourself'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Shop for daily necessities yourself'], prefix='Shop')], axis=1)
star_data_dummied = star_data_dummied.drop(['Shop for daily necessities yourself'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Exercise function'], prefix='Exercise')], axis=1)
star_data_dummied = star_data_dummied.drop(['Exercise function'], axis=1)
star_data_dummied = pd.concat([star_data_dummied, pd.get_dummies(star_data['Nutrition'], prefix='Nutrition')], axis=1)
star_data_dummied = star_data_dummied.drop(['Nutrition'], axis=1)
star_data_dummied

##############


### Scale features

data_scaled = star_data_dummied.copy()
scaler = StandardScaler()
scaler.fit(star_data_dummied[data_scaled.columns[2:24]])
data_scaled[data_scaled.columns[2:24]] = scaler.transform(data_scaled[data_scaled.columns[2:24]])

##############


### Group time

data_scaled_means = data_scaled.drop([' Measure Date Time'], axis = 1).groupby('SubjectID').mean()

##############


### Machine learning models
## Isolation Forest

ilf = IsolationForest().fit(data_scaled_means)
answerIF_proba = abs(ilf.score_samples(data_scaled_means))
answerIF_proba = pd.DataFrame({'target' : answerIF_proba})

## Local Outlier Factor

lof = LocalOutlierFactor(n_neighbors=2, novelty=True)
lof.fit(data_scaled_means)
answerLOF_proba = lof.decision_function(data_scaled_means)
answerLOF_proba = 1 - ((answerLOF_proba - answerLOF_proba.min()) / (answerLOF_proba.max() - answerLOF_proba.min())) 
answerLOF_proba = pd.DataFrame({'target' : answerLOF_proba})

## Elliptic Envelope

ee = EllipticEnvelope()
ee.fit(data_scaled_means)
answerEE_proba = ee.decision_function(data_scaled_means)
answerEE_proba = 1 - (answerEE_proba - 3 * answerEE_proba.min()) * 10 ** 12
answerEE_proba = pd.DataFrame({'target' : answerEE_proba})

##############


### Soft voting

voting_answer = pd.DataFrame({'target' : ((answerIF_proba*2 + answerLOF_proba*1 + answerEE_proba*2) / 5).T.apply(lambda x: -1 if x.values[0]>0.4 else 1 )}) 

##############


voting_answer.to_csv('../../data/answer.csv', index=False)
