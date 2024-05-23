from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import matplotlib.pyplot as plt
import seaborn as sns

# from datetime import datetime
#import polars as pl
from pandas import DataFrame

# import os
import warnings

# import sys
from dotenv import load_dotenv

load_dotenv(".env")
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.linear_model import ElasticNet
from urllib.parse import urlparse
import mlflow
from pandas import DataFrame, read_html, get_dummies
import logging

# import pandas as pd
import re
import nltk

# import mlflow
# import numpy as np
from nltk.stem import WordNetLemmatizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report, accuracy_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier

pd.set_option("display.max_colwidth", None)

nltk.download("wordnet")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
alpha = 0.9
l1_ratio = 1.0


COMPUTE_KIND = "Mlflow"
LAYER = "ml"

lemmatizer = WordNetLemmatizer()


def clean_text(text):
    text = text.lower()
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


@asset(
    description="extract data from platium for machingleaning",
    ins={
        "silver_cleaned_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
    },
    key_prefix=["ml", "extract"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def extract(
    context,
    silver_cleaned_order_review,
):
    df1 = silver_cleaned_order_review
    df = df1.toPandas()

    df_comments = df.loc[:, ["review_score", "review_comment_message"]]
    df_comments.columns = ["score", "comment"]

    df_comments["comment"] = df_comments["comment"].apply(lambda x: clean_text(x))

    mapping = {
        1: "negative",
        2: "negative",
        3: "negative",
        4: "positive",
        5: "positive",
    }
    df_comments["score"] = df_comments["score"].map(mapping)

    text_vectorizer = TfidfVectorizer(max_features=15000, use_idf=True, smooth_idf=True)

    X = df_comments["comment"]
    y = df_comments["score"]
    X_pre = text_vectorizer.fit_transform(X)

    param_grid = {
        "C": [0.001, 0.01, 0.1, 1.0, 10.0],
        "penalty": ["l1", "l2"],
        "solver": ["liblinear", "saga"],
    }

    X_train, X_test, y_train, y_test = train_test_split(
        X_pre, y, stratify=y, train_size=0.8, random_state=1
    )
    # GridSearchCV
    log = LogisticRegression(max_iter=1000)
    grid_search = GridSearchCV(estimator=log, param_grid=param_grid, scoring='accuracy', cv=5)
    grid_search.fit(X_train, y_train)

    best_params = grid_search.best_params_
    """Mlflow"""
    mlflow.set_tracking_uri("http://mlflow_server:5000")
    experiment_name = "sentiment analysis"
    mlflow.set_experiment(experiment_name)
    mlflow.start_run()
    mlflow.sklearn.autolog()
    #log = LogisticRegression(max_iter=1000, solver="saga", C=1, penalty="l2")
    # GridSearchCV
    log = LogisticRegression(max_iter=1000, **best_params)
    # GridSearchCV
    log.fit(X_train, y_train)
    y_pred = log.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    with open("classification_report.txt", "w") as f:
        f.write(report)
    cm = confusion_matrix(y_pred, y_test)
    plt.figure(figsize=(8, 6))
    sns.heatmap(
        cm,
        annot=True,
        cmap="Blues",
        fmt="g",
        xticklabels=["negative", "positive"],
        yticklabels=["negative", "positive"],
    )
    plt.xlabel("Actual labels")
    plt.ylabel("Predict labels")
    plt.title("Confusion Matrix")
    plt.savefig("confusion_matrix.png")
    mlflow.log_metrics({"test_accuracy": acc})
    mlflow.log_artifact("confusion_matrix.png")
    mlflow.log_artifact("classification_report.txt")
    mlflow.sklearn.log_model(text_vectorizer, "TF-IDFVectorizer")
    mlflow.end_run()

