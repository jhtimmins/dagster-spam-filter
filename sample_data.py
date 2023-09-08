import random

import duckdb

# Define spam and non-spam words
spam_words = ["urgent", "winner", "guarantee", "congratulations", "prize", "free"]
non_spam_words = ["hello", "meeting", "schedule", "update", "invoice", "reminder"]

# Define templates for spam and non-spam messages
spam_templates = [
    "You are the {0}!",
    "Claim your {1} {0} now!",
    "{1} {0} offer!",
    "This is an {0} message!",
]
non_spam_templates = [
    "{0}, can we have a {1}?",
    "{1} on your {0}.",
    "{0} {1} for the week.",
]

# Connect to DuckDB
conn = duckdb.connect("database.duckdb")

# Create dimension table if it doesn't exist
conn.execute(
    """
CREATE TABLE IF NOT EXISTS messages (
    spam BOOLEAN,
    body VARCHAR
);
"""
)

# Initialize list to hold fake messages and labels
fake_messages = []
fake_labels = []

# Generate 10,000 fake messages
for _ in range(10000):
    is_spam = random.choice([True, False])

    if is_spam:
        template = random.choice(spam_templates)
        words_needed = [random.choice(spam_words) for _ in range(template.count("{"))]
        message = template.format(*words_needed)
    else:
        template = random.choice(non_spam_templates)
        words_needed = [
            random.choice(non_spam_words) for _ in range(template.count("{"))
        ]
        message = template.format(*words_needed)

    fake_messages.append(message)
    fake_labels.append(is_spam)

# Insert the fake data into the messages table
for message, is_spam in zip(fake_messages, fake_labels):
    conn.execute("INSERT INTO messages (spam, body) VALUES (?, ?)", (is_spam, message))

# Commit changes to the database
conn.commit()

# Verify data insertion
result = conn.execute("SELECT * FROM messages LIMIT 10").fetchall()
print(result)
