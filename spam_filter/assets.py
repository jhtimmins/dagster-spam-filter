import string
import time
from collections import Counter
from typing import Dict

import numpy as np
import pandas as pd
from dagster import AssetOut, asset, multi_asset
from sklearn.model_selection import train_test_split

from . import spam
from .resources import Database, ModelStorage


@asset
def sms_spam_examples() -> pd.DataFrame:
    spam_df = pd.read_csv("./spam.csv", encoding="ISO-8859-1", usecols=["v1", "v2"])
    spam_df = spam_df.rename(columns={"v1": "spam", "v2": "text"})
    spam_df["spam"] = spam_df["spam"] == "spam"
    return spam_df


@asset
def user_messages(database: Database) -> pd.DataFrame:
    results = database.query(
        """
        SELECT
            spam,
            body
        FROM
            messages
    """
    )
    results.rename(columns={"body": "text"}, inplace=True)
    return results


@asset
def spam_dataset(sms_spam_examples, user_messages) -> pd.DataFrame:
    joined_dataframe = pd.concat([sms_spam_examples, user_messages], ignore_index=True)
    return joined_dataframe.sample(frac=1).reset_index(drop=True)


@multi_asset(
    outs={
        "spam_bag_of_words": AssetOut(),
        "non_spam_bag_of_words": AssetOut(),
        "spam_training_dataframe": AssetOut(),
        "spam_testing_dataframe": AssetOut(),
    }
)
def spam_model(spam_dataset: pd.DataFrame):
    def preprocess_text(text: str) -> str:
        return text.lower().translate(str.maketrans("", "", string.punctuation))

    def calculate_word_frequencies(words: list, shared_words: set) -> dict:
        word_counts = Counter(words)
        total_words = len(words)
        return {word: word_counts[word] / total_words for word in shared_words}

    spam_dataset["text"] = spam_dataset["text"].apply(preprocess_text)
    spam_dataset = spam_dataset.sample(frac=1)

    train_data, test_data = train_test_split(spam_dataset, test_size=0.3)

    spam_training_words = " ".join(train_data[train_data["spam"]].text).split()
    non_spam_training_words = " ".join(train_data[~train_data["spam"]].text).split()

    shared_words = set(spam_training_words).intersection(set(non_spam_training_words))
    spam_bag_of_words = calculate_word_frequencies(spam_training_words, shared_words)
    non_spam_bag_of_words = calculate_word_frequencies(
        non_spam_training_words, shared_words
    )

    return spam_bag_of_words, non_spam_bag_of_words, train_data, test_data


@asset
def model_score(
    spam_bag_of_words: Dict,
    non_spam_bag_of_words: Dict,
    spam_training_dataframe: pd.DataFrame,
    spam_testing_dataframe: pd.DataFrame,
) -> Dict:
    def get_prediction_rate(predictions: pd.Series, true_values: pd.Series) -> float:
        return np.sum(predictions & true_values) / np.sum(true_values)

    def get_spam_probability(text: str) -> float:
        return spam.compare_text_to_bow(
            text.split(),
            spam_bag_of_words=spam_bag_of_words,
            non_spam_bag_of_words=non_spam_bag_of_words,
            percent_spam=percent_spam,
        )

    percent_spam = spam_training_dataframe.spam.mean()
    predictions = spam_testing_dataframe.text.apply(get_spam_probability)

    true_positives = get_prediction_rate(predictions, spam_testing_dataframe.spam)
    false_positives = get_prediction_rate(predictions, ~spam_testing_dataframe.spam)

    return {
        "true_positives": true_positives,
        "false_positives": false_positives,
        "percent_spam": percent_spam,
    }


@asset
def model_file(
    model_score: Dict,
    spam_bag_of_words: Dict,
    non_spam_bag_of_words: Dict,
    database: Database,
    model_storage: ModelStorage,
):
    df = database.query("SELECT * FROM models WHERE status = 'active'")
    if (
        df.empty
        or (df.iloc[0].true_positive_rate <= model_score["true_positives"])
        and (df.iloc[0].false_positive_rate >= model_score["false_positives"])
    ):
        filename = f"{int(time.time())}.json"
        model_storage.write(
            filename,
            {
                "spam_bow": spam_bag_of_words,
                "non_spam_bow": non_spam_bag_of_words,
                "percent_spam": model_score["percent_spam"],
            },
        )
        with database as conn:
            conn.execute(
                "UPDATE models SET status = 'inactive' WHERE status = 'active'"
            )
            conn.execute(
                f"""
                INSERT INTO models (path, status, true_positive_rate, false_positive_rate) 
                VALUES ('{filename}', 'active', {model_score["true_positives"]}, {model_score["false_positives"]})
                """
            )
