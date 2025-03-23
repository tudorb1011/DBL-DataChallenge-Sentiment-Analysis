from textblob import TextBlob
import pandas as pd
import json
import os
import sqlite3
import time
import dbl_constants
"""Processes using textblob. you must install this package yourself.
        TextBlob is  available as a conda package. To install with conda, run
        $ conda install -c conda-forge textblob
        $ python -m textblob.download_corpora

Requires: the database with interactions table
Output:   Replaces interactions table with new one which has sentiment analyzed
          Views of the interactions table with text included
        """

# Set constants
airlines = dbl_constants.airlines
database_path = "temporary_database.sqlite3"

# connect
conn = sqlite3.connect(database_path)
cur = conn.cursor()

# get columns per table
cur.execute("""PRAGMA table_info(TWEETS);""")
tweet_columns = tuple(t[1] for t in cur.fetchall())
cur.execute("""PRAGMA table_info(USERS);""")
user_columns = tuple(t[1] for t in cur.fetchall())
cur.execute("""PRAGMA table_info(INTERACTIONS);""")
interaction_columns = tuple(t[1] for t in cur.fetchall())


def tb_enrich(ls):
    # Enriches a column of text with TextBlob Sentiment Analysis outputs
    tb_polarity = []
    tb_subject = []
    for tweet in ls:
        sentiment = TextBlob(tweet).sentiment
        tb_polarity.append(sentiment[0])
        tb_subject.append(sentiment[1])
    return tb_polarity, tb_subject


def create_interactions_with_text_sqlite_view():
    print("CREATING SQLITE INTERACTIONS WITH TEXT VIEW")
    cur.execute("DROP VIEW IF EXISTS _interactions_with_texts_part_1")
    cur.execute("DROP VIEW IF EXISTS _interactions_with_texts_part_2")
    cur.execute("DROP VIEW IF EXISTS INTERACTIONS_WITH_TEXT")
    print("getting interactions and tweet texts")
    cur.execute("""
        CREATE VIEW _interactions_with_texts_part_1
        AS
        SELECT  INTERACTIONS.*, 
                case
                    WHEN TWEETS.extended_fulltext IS NOT NULL
                        THEN TWEETS.extended_fulltext
                    ELSE TWEETS.text END as tweet_text
        FROM INTERACTIONS, TWEETS
        WHERE TWEETS.id == INTERACTIONS.tweet_id""")
    print("getting response text")
    cur.execute("""
        CREATE VIEW _interactions_with_texts_part_2
        AS
        SELECT  _interactions_with_texts_part_1.*, 
                case
                    WHEN TWEETS.extended_fulltext IS NOT NULL
                        THEN TWEETS.extended_fulltext
                    ELSE TWEETS.text END as response_text
        FROM _interactions_with_texts_part_1, TWEETS
        WHERE TWEETS.id == _interactions_with_texts_part_1.response_id""")
    print("getting all texts")
    cur.execute("""
        CREATE VIEW INTERACTIONS_WITH_TEXT
        AS
        SELECT  _interactions_with_texts_part_2.*, 
                case
                    WHEN TWEETS.extended_fulltext IS NOT NULL
                        THEN TWEETS.extended_fulltext
                    ELSE TWEETS.text END as reaction_text
        FROM _interactions_with_texts_part_2, TWEETS
        WHERE TWEETS.id == _interactions_with_texts_part_2.reaction_id""")

create_interactions_with_text_sqlite_view()
cur.execute("SELECT * FROM INTERACTIONS_WITH_TEXT")
interactions = pd.DataFrame(cur.fetchall(), columns=list(interaction_columns) + ["tweet_text",
                                                                                 "response_text",
                                                                                 "reaction_text"])
# calculate polarity, subjectivity
print("performing sentiment analysis on initial tweets: 1")
interactions["tweet_polar"],    interactions["tweet_subj"] = tb_enrich(list(interactions["tweet_text"]))
print("performing sentiment analysis on response tweets: 2")
interactions["response_polar"], interactions["response_subj"] = tb_enrich(list(interactions["response_text"]))
print("performing sentiment analysis on reaction tweets: 3")
interactions["reaction_polar"], interactions["reaction_subj"] = tb_enrich(list(interactions["reaction_text"]))
# ... and how it differs in the initial and reaction tweet
interactions["polarity_change"] = interactions["reaction_polar"] - interactions["tweet_polar"]
# Drop the columns which contain duplicate info (such as text from TWEETS table)
print("replacing table with new one")
interactions.drop(["tweet_text", "response_text", "reaction_text"], axis=1, inplace=True)
interactions.to_sql(name="INTERACTIONS", con=conn, index=False, if_exists='replace')

# Create interactions with text view again, this view can be used later
# we have to recreate it because INTERACTIONS table has been overwritten
create_interactions_with_text_sqlite_view()
# additionaly create view which also contains tweet DateTimes
print(" creating additional view with tweet creation times")
cur.execute("DROP VIEW IF EXISTS EXPANDED_INTERACTIONS")
cur.execute("DROP VIEW IF EXISTS _expanded_interactions_part_1")
cur.execute("DROP VIEW IF EXISTS _expanded_interactions_part_2")
cur.execute("""
CREATE VIEW _expanded_interactions_part_1
AS 
SELECT TWEETS.created_at as created_at_initial, INTERACTIONS_WITH_TEXT.* 
            FROM TWEETS, INTERACTIONS_WITH_TEXT
            WHERE TWEETS.id == INTERACTIONS_WITH_TEXT.tweet_id
""")
cur.execute("""
CREATE VIEW _expanded_interactions_part_2
AS 
SELECT TWEETS.created_at as created_at_response, _expanded_interactions_part_1.* 
            FROM TWEETS, _expanded_interactions_part_1
            WHERE TWEETS.id == _expanded_interactions_part_1.response_id
""")
cur.execute("""
CREATE VIEW EXPANDED_INTERACTIONS
AS 
SELECT TWEETS.created_at as created_at_reaction, _expanded_interactions_part_2.* 
            FROM TWEETS, _expanded_interactions_part_2
            WHERE TWEETS.id == _expanded_interactions_part_2.reaction_id
""")
conn.close()
#





