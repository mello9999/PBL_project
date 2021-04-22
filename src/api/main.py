from fastapi import Request, FastAPI

import pickle
import numpy as np
import pandas as pd
import math
import copy

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

spark = SparkSession \
    .builder \
    .appName("Spark Dataframe") \
    .config("spark.some.config.option") \
    .getOrCreate()



def cleaning(star_data):
    if star_data['Age'] == "None":
        star_data['Age'] = 73.46666666666667
    if star_data['Gender'] == "None":
        star_data['Gender'] = "Female"
    if star_data['Height'] == "None":
        star_data['Height'] = 157.41372549019607
    if star_data['Body weight'] == "None":
        star_data['Body weight'] = 56.13529411764706
    if star_data['Drinking'] == "None":
        star_data['Drinking'] = "no"
    if star_data[' Sleep Hour'] == "None":
        star_data[' Sleep Hour'] = 0.0
    if star_data['Total cholesterol'] == "None":
        star_data['Total cholesterol'] = 212.0839160839164
    if star_data['LDL cholesterol'] == "None":
        star_data['LDL cholesterol'] = 127.23913043478282
    if star_data['HDL cholesterol'] == "None":
        star_data['HDL cholesterol'] = 61.23913043478259
    if star_data['HbA1c'] == "None":
        star_data['HbA1c'] = 5.736448598130842
    if star_data['AST'] == "None":
        star_data['AST'] = 21.153846153846132
    if star_data['ALT'] == "None":
        star_data['ALT'] = 18.335664335664333
    if star_data['LDH'] == "None":
        star_data['LDH'] = 156.92125984251945
    if star_data['Na'] == "None":
        star_data['Na'] = 143.2051282051283
    if star_data['K'] == "None":
        star_data['K'] = 4.040598290598289
    if star_data['Diabetes mellitus'] == "None":
        star_data['Diabetes mellitus'] = "Nothing"
    if star_data[' Systolic Pressure'] == "None":
        star_data[' Systolic Pressure'] = 258.59740259740136
    if star_data[' Diastolic Pressure'] == "None":
        star_data[' Diastolic Pressure'] = 197.292207792207
    if star_data[' Mean Arterial Pressure'] == "None":
        star_data[' Mean Arterial Pressure'] = 231.08441558441612
    if star_data[' Pulse Rate'] == "None":
        star_data[' Pulse Rate'] = 184.17532467532385
    if star_data[' Irregular Pulse Flag'] == "None":
        star_data[' Irregular Pulse Flag'] = " false"
    if star_data['Blood sugar'] == "None":
        star_data['Blood sugar'] = 97.2321428571424
    if star_data['I can walk for more than 15 minutes'] == "None":
        star_data['I can walk for more than 15 minutes'] = "no"
    if star_data['I am actively going out'] == "None":
        star_data['I am actively going out'] = "no"
    if star_data['Do the cleaning and washing yourself'] == "None":
        star_data['Do the cleaning and washing yourself'] = "no"
    if star_data['Shop for daily necessities yourself'] == "None":
        star_data['Shop for daily necessities yourself'] = "no"
    
    return star_data
    
def dummies(data):
    star_data =  copy.deepcopy(data) 
    c = ['Gender_Female', 'Gender_Man', 'Drinking_no', 'Drinking_yes', 'Antihypertensive_Amlodipine', 
       'Antihypertensive_Azilva、Amlodipine', 'Antihypertensive_None',
       'Antihypertensive_Olmesartan, Nifedipine', 
       'Antihypertensive_Trichlormethiazide、Amlodipine',
       'Antihypertensive_Valsartan、Nifedipine', 'Osteoporosis_Alendronic acid',
       'Osteoporosis_None', 'Antidiabetic_Gractive', 'Antidiabetic_None',
       'Antidiabetic_Tenelia,Metoformin hydrochloride,Repaglinide,',
       'Diabetes_Nothing', 'Diabetes_Yes', 'Irregular_Pulse_Flag_ false',
       'Irregular_Pulse_Flag_ true', 'Appetite_ a lot', 'Appetite_None',
       'Appetite_some', 'Preference_I like light taste',
       'Preference_I like sweets', 'Preference_None', 'Sleep_Quest_None',
       'Sleep_Quest_Sleeping ', 'Sleep_Quest_Sleeping well', 'Anxiety_ a lot',
       'Anxiety_None', 'Anxiety_some', 'up/down stairs_None',
       'up/down stairs_yes', 'walk_no', 'walk_yes', 'going_out_no',
       'going_out_yes', 'cleaning_no', 'cleaning_yes', 'Shop_no', 'Shop_yes',
       'Exercise_None', 'Exercise_Observation required', 'Exercise_good',
       'Nutrition_None', 'Nutrition_Observation required', 'Nutrition_good']
    for i in c :
        star_data[i] = 0

    star_data['Gender' + "_" + star_data['Gender']] = 1
    del star_data['Gender']

    star_data['Drinking' + "_" + star_data['Drinking']] = 1
    del star_data['Drinking']

    star_data['Antihypertensive' + "_" + star_data['Antihypertensive']] = 1
    del star_data['Antihypertensive']

    star_data['Osteoporosis' + "_" + star_data['Osteoporosis drug']] = 1
    del star_data['Osteoporosis drug']

    star_data['Antidiabetic' + "_" + star_data['Antidiabetic drug']] = 1
    del star_data['Antidiabetic drug']

    star_data['Diabetes' + "_" + star_data['Diabetes mellitus']] = 1
    del star_data['Diabetes mellitus']

    star_data['Irregular_Pulse_Flag' + "_" + star_data[' Irregular Pulse Flag']] = 1
    del star_data[' Irregular Pulse Flag']

    star_data['Appetite' + "_" + star_data['Appetite Questionnaire results']] = 1
    del star_data['Appetite Questionnaire results']

    star_data['Preference' + "_" + star_data['Preference Questionnaire results']] = 1
    del star_data['Preference Questionnaire results']

    star_data['Sleep_Quest' + "_" + star_data['Sleep Questionnaire results']] = 1
    del star_data['Sleep Questionnaire results']

    star_data['Anxiety' + "_" + star_data['Anxiety about health Questionnaire results']] = 1
    del star_data['Anxiety about health Questionnaire results']

    star_data['up/down stairs' + "_" + star_data['I can go up and down stairs without being transmitted to the railing or wall']] = 1
    del star_data['I can go up and down stairs without being transmitted to the railing or wall']

    star_data['walk' + "_" + star_data['I can walk for more than 15 minutes']] = 1
    del star_data['I can walk for more than 15 minutes']

    star_data['going_out' + "_" + star_data['I am actively going out']] = 1
    del star_data['I am actively going out']

    star_data['cleaning' + "_" + star_data['Do the cleaning and washing yourself']] = 1
    del star_data['Do the cleaning and washing yourself']

    star_data['Shop' + "_" + star_data['Shop for daily necessities yourself']] = 1
    del star_data['Shop for daily necessities yourself']

    star_data['Exercise' + "_" + star_data['Exercise function']] = 1
    del star_data['Exercise function']

    star_data['Nutrition' + "_" + star_data['Nutrition']] = 1
    del star_data['Nutrition']


    data_scaled = star_data.copy()
    scaler_file = open("../../data/model/scaler", "rb")
    scaler = pickle.load(scaler_file)
    scaler_file.close()
    
    m = scaler.mean_
    v = scaler.var_
    s = scaler.scale_

    co = [" BMI", " Body Age", "Age", "Height", "Body weight", " Sleep Hour", 
          " Body Fat Percentage", " Basal Metabolism", "Total cholesterol", 
          "LDL cholesterol", "HDL cholesterol", "HbA1c", "AST", "ALT", 
          "LDH", "Na", "K", " Systolic Pressure", " Diastolic Pressure", 
          " Mean Arterial Pressure", " Pulse Rate", "Blood sugar"]

    for i, key in enumerate(co):  
      data_scaled[key] = float(((data_scaled[key] - m[i]) / v[i]) * s[i])
      
    return data_scaled

@udf(returnType=DoubleType())
def predictor(*kwargs):
    def sigmoid(x): 
        if x > 1000000000:
            return 1
        if x < -21:
            return 0
        return 1 / (1 + math.exp(-x))

    def check_more_one(x):
        if x > 1:
            return 1
        else:
            return x

    #open picked model
    model = open("../../data/model/IsolationForest", "rb")
    ilf_model = pickle.load(model)
    model.close()
    answerIF_proba = abs(ilf_model.score_samples([kwargs]))


 
    model = open("../../data/model/LocalOutlierFactor", "rb")
    lof_model = pickle.load(model)
    model.close()
    answerLOF_proba = lof_model.decision_function([kwargs])
    answerLOF_proba = 1 - ((answerLOF_proba + 0.9118517621467248) / (0.5391638200654012 + 0.9118517621467248)) 

    model = open("../../data/model/EllipticEnvelope", "rb")
    ee_model = pickle.load(model)
    model.close()
    answerEE_proba = ee_model.decision_function([kwargs])
    answerEE_proba = list(map(sigmoid, answerEE_proba))[0]

    result = (float(answerIF_proba*2) + float(answerLOF_proba*1) + float(answerEE_proba*2)) / 5
    result = check_more_one(result)

    return result

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/predict/")
async def predict(body: dict):
    results = dict()
    

    co = [' BMI', ' Body Age', 'Age', 'Height',
       'Body weight', ' Sleep Hour', ' Body Fat Percentage',
       ' Basal Metabolism', 'Total cholesterol', 'LDL cholesterol',
       'HDL cholesterol', 'HbA1c', 'AST', 'ALT', 'LDH', 'Na', 'K',
       ' Systolic Pressure', ' Diastolic Pressure', ' Mean Arterial Pressure',
       ' Pulse Rate', 'Blood sugar', 'Gender_Female', 'Gender_Man',
       'Drinking_no', 'Drinking_yes', 'Antihypertensive_Amlodipine',
       'Antihypertensive_Azilva、Amlodipine', 'Antihypertensive_None',
       'Antihypertensive_Olmesartan, Nifedipine',
       'Antihypertensive_Trichlormethiazide、Amlodipine',
       'Antihypertensive_Valsartan、Nifedipine', 'Osteoporosis_Alendronic acid',
       'Osteoporosis_None', 'Antidiabetic_Gractive', 'Antidiabetic_None',
       'Antidiabetic_Tenelia,Metoformin hydrochloride,Repaglinide,',
       'Diabetes_Nothing', 'Diabetes_Yes', 'Irregular_Pulse_Flag_ false',
       'Irregular_Pulse_Flag_ true', 'Appetite_ a lot', 'Appetite_None',
       'Appetite_some', 'Preference_I like light taste',
       'Preference_I like sweets', 'Preference_None', 'Sleep_Quest_None',
       'Sleep_Quest_Sleeping ', 'Sleep_Quest_Sleeping well', 'Anxiety_ a lot',
       'Anxiety_None', 'Anxiety_some', 'up/down stairs_None',
       'up/down stairs_yes', 'walk_no', 'walk_yes', 'going_out_no',
       'going_out_yes', 'cleaning_no', 'cleaning_yes', 'Shop_no', 'Shop_yes',
       'Exercise_None', 'Exercise_Observation required', 'Exercise_good',
       'Nutrition_None', 'Nutrition_Observation required', 'Nutrition_good']
    for key in body:
        b = dummies(cleaning(body[key]))
        column_input = []
        for k in co:
            column_input.append(b[k])

        column_input = spark.sparkContext.parallelize([column_input]).toDF()

        df_prediction = column_input.withColumn("prediction",
                                                    predictor(*column_input))
        result = [list(row)[0] for row in df_prediction.select('prediction').collect()]
        results[key] = result
    return results
