from fastapi import Request, FastAPI

import pickle
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

spark = SparkSession \
    .builder \
    .appName("Spark Dataframe") \
    .config("spark.some.config.option") \
    .getOrCreate()

@udf(returnType=DoubleType())
def predictor(*kwargs):
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
    answerEE_proba = 1 - (answerEE_proba - 3 * -1.6697754290362354e-13) * 10 ** 12

    return (float(answerIF_proba*2) + float(answerLOF_proba*1) + float(answerEE_proba*2)) / 5

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/predict/")
async def predict(body: dict):
    column_input = []
    for c in body:
        column_input.append(body[c])
    column_input = spark.sparkContext.parallelize([column_input]).toDF()
    df_prediction = column_input.withColumn("prediction",
                                                predictor(*column_input))
    result = [list(row)[0] for row in df_prediction.select('prediction').collect()]
    
    return result
