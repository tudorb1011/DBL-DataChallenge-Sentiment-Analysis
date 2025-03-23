# make hashtags dataframe first and then implement in database as table
# hashtag as primary key with list of tweet_ids that use it and count of how many times it occurs

import pandas as pd

import sqlite3

import time

database_path = "temporary_database.sqlite3"

start = time.time()
conn = sqlite3.connect(database_path)
cur1 = conn.cursor()
cur1.execute("""SELECT * FROM TWEETS WHERE lang == 'en' AND hashtags != ''""")
tweet_df = pd.DataFrame(cur1.fetchall(),
                        columns=['created_at', 'id', 'text', 'extended_fulltext', 'source',
                                 'truncated', 'in_reply_to_status_id', 'in_reply_to_user_id', 'user_id',
                                 'is_quote_status', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count',
                                 'favorited', 'retweeted', 'filter_level', 'lang', 'timestamp_ms', 'quoted_status_id',
                                 'hashtags', 'user_mentions'])
hashtags_df = pd.DataFrame(columns=['hashtag', 'tweet_ids', 'count'])
i = 0
# now hashtags are stored as strings with smaller strings in them, not sure how to iterate over those to make a df
# maybe a list would be easier to use?
for index, row in tweet_df.iterrows():
    id = row['id']
    # hashtags are stored as a list without brackets, so basically a string with smaller strings
    # the for loop makes every set of hashtags into a list, but it still has the extra ''s
    # so to get rid of those, the s[1:-1] gets rid of the first and last character
    hashtags = list(s[1:-1] for s in list(row['hashtags'].split(", ")))
    for hashtag in hashtags:
        hashtag = hashtag.lower()
        if hashtag not in hashtags_df['hashtag'].values:
            hashtags_df.at[i, 'hashtag'] = hashtag
            hashtags_df.at[i, 'tweet_ids'] = [id]
            hashtags_df.at[i, 'count'] = 1
            i += 1
        else:
            hashtags_df.at[hashtags_df[hashtags_df['hashtag'] == hashtag].index.tolist()[0], 'tweet_ids'].append(id)
            hashtags_df.at[hashtags_df[hashtags_df['hashtag'] == hashtag].index.tolist()[0], 'count'] += 1
hashtags_df['tweet_ids'] = hashtags_df['tweet_ids'].astype(str)
print(hashtags_df)
hashtags_df.to_sql(name="HASHTAGS", con=conn, index=False, if_exists='replace')
print(hashtags_df[hashtags_df['hashtag'] == 'klm'])
print("took", round(time.time() - start), "seconds")

