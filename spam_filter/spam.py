import json

import duckdb
import numpy as np


def is_spam(text):
    with duckdb.connect("./database.duckdb") as conn:
        df = conn.execute("SELECT * FROM models WHERE status = 'active'").fetchdf()
        path_value = df.loc[0, "path"]
        with open(f"./weights/{path_value}", "r") as f:
            data = json.load(f)
            spam_bow = data.get("spam_bow")
            non_spam_bow = data.get("non_spam_bow")
            percent_spam = data.get("percent_spam")

    return compare_text_to_bow(text.split(), spam_bow, non_spam_bow, percent_spam)


def compare_text_to_bow(text, spam_bag_of_words, non_spam_bag_of_words, percent_spam):
    valid_words = [word for word in text if word in spam_bag_of_words]

    spam_probs, non_spam_probs = [], []
    for word in valid_words:
        spam_probs.append(spam_bag_of_words[word])
        non_spam_probs.append(non_spam_bag_of_words[word])

    spam_score = sum(map(np.log, spam_probs)) + np.log(percent_spam)
    non_spam_score = sum(map(np.log, non_spam_probs)) + np.log(1 - percent_spam)

    return bool(non_spam_score < spam_score)
