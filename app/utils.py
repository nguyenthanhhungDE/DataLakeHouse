import streamlit as st
import pandas as pd
import mlflow
from mlflow.sklearn import load_model
import time
import mysql.connector
import re
from nltk.stem import WordNetLemmatizer
import nltk

nltk.download("wordnet")
# mlflow.set_tracking_uri("http://minio:9000")

# uri = "s3://mlflow/8/a4e8c20ccb2c449f902fc6c955e3f298/artifacts/model"
# model = mlflow.sklearn.load_model(uri)

# t_uri = "s3://mlflow/8/a4e8c20ccb2c449f902fc6c955e3f298/artifacts/text_vectorizer"

# vector = mlflow.sklearn.load_model(t_uri)

mlflow.set_tracking_uri("http://mlflow_server:5000")

model_name = "classification"
model_version = 1

vector_name = "transform"
vector_version = 1

model = mlflow.sklearn.load_model(model_uri=f"models:/{model_name}/{model_version}")
vector = mlflow.sklearn.load_model(model_uri=f"models:/{vector_name}/{vector_version}")


def predict(text):
    text_vectorized = vector.transform([text])
    prediction = model.predict(text_vectorized)
    proba = model.predict_proba(text_vectorized)
    max_proba = max(proba[0])
    return prediction, max_proba


lemmatizer = WordNetLemmatizer()


def handle(text):
    text = text.lower()
    # text = re.sub(
    #     r"[^a-zA-Z?.!,¿\s]+", " ", text
    # )
    text = re.sub(r"http\S+", "", text)
    html = re.compile(r"<.*?>")
    text = html.sub(r"", text)
    punctuations = "@#!?+&*[]-%.:/();$=><|{}^" + "'`" + "_"
    for p in punctuations:
        text = text.replace(p, "")
    # text = [word.lower() for word in text.split() if word.lower() not in sw]
    text = [word.lower() for word in text.split()]
    text = [lemmatizer.lemmatize(word) for word in text]
    text = " ".join(text)
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    text = emoji_pattern.sub(r"", text)

    return text
