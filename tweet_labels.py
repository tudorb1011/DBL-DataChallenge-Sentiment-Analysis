# making a dataframe that contains manually labelled tweets ranging from negative to neutral to positive for later
# comparison to sentiment analysis model
# negative = -1; leaning negative = -0.5; neutral = 0; leaning positive = 0.5; positive = 1
# difference between initial tweet and reaction

# mildly and extremely negative or positive

# in analysing customer service of the airlines, maybe it's interesting to weigh changes from negative to positive more
# than from neutral or positive; also a graph of sentiment change would be cool for presentation

# there seem to be some double tweets in labels_df, maybe also in interactions?
import pandas as pd

import sqlite3
test_tweets_amount = 150  # should be 100, but can make it smaller to test the script

database_path = "temporary_database.sqlite3"

conn = sqlite3.connect(database_path)
cur1 = conn.cursor()
cur1.execute("""SELECT tweet_id, response_id, reaction_id, user_1, airline, user_3
FROM INTERACTIONS
ORDER BY RANDOM()""")
interactions_df = pd.DataFrame(cur1.fetchall(),
                               columns=['tweet_id', 'response_id', 'reaction_id', 'user_1', 'airline', 'user_3'])
cur2 = conn.cursor()
cur2.execute("""SELECT id, text, extended_fulltext FROM TWEETS WHERE lang == 'en'""")  # why in that month?
tweet_df = pd.DataFrame(cur2.fetchall(),
                        columns=['id', 'text', 'extended_fulltext'])

# position = from tweet_id, response_id or reaction_id
label_df = pd.DataFrame(columns=['interaction_nr', 'position', 'user_id', 'text', 'extended_fulltext', 'label',
                                 'diff_after_response'])
# i is for getting to next row in label_df, n is to count interaction_nr, k is for the break
# to make sure there's only 100 tweets to label
i = 0
n = 1
k = 1
for index, interaction in interactions_df.iterrows():
    if k > test_tweets_amount:
        break
    if interaction['tweet_id'] in tweet_df['id'].values and interaction['response_id'] in tweet_df['id'].values and \
            interaction['reaction_id'] in tweet_df['id'].values:
        label_df.at[i, 'interaction_nr'] = n
        label_df.at[i, 'position'] = 'from tweet_id'
        label_df.at[i, 'user_id'] = int(interaction['user_1'])
        label_df.at[i, 'text'] = tweet_df['text'][tweet_df['id'] == interaction['tweet_id']].item()
        label_df.at[i, 'extended_fulltext'] = tweet_df['extended_fulltext'][tweet_df['id'] == interaction[
            'tweet_id']].item()
        i += 1
        k += 1
        label_df.at[i, 'interaction_nr'] = n
        label_df.at[i, 'position'] = 'from response_id'
        label_df.at[i, 'user_id'] = int(interaction['airline'])
        label_df.at[i, 'text'] = tweet_df['text'][tweet_df['id'] == interaction['response_id']].item()
        label_df.at[i, 'extended_fulltext'] = tweet_df['extended_fulltext'][
            tweet_df['id'] == interaction['response_id']].item()
        i += 1
        label_df.at[i, 'interaction_nr'] = n
        label_df.at[i, 'position'] = 'from reaction_id'
        label_df.at[i, 'user_id'] = int(interaction['user_3'])
        label_df.at[i, 'text'] = tweet_df['text'][tweet_df['id'] == interaction['reaction_id']].item()
        label_df.at[i, 'extended_fulltext'] = tweet_df['extended_fulltext'][
            tweet_df['id'] == interaction['reaction_id']].item()
        i += 1
        k += 1
    n += 1
for index, tweet in label_df.iterrows():
    # skips airline responses, because (i think) they don't need to be labelled
    if tweet['extended_fulltext'] is not None and tweet['position'] != 'from response_id':
        print(tweet['extended_fulltext'], ";The position of this tweet in the interaction is {}".format(
            tweet['position']))
        value = input("Please type -1, -0.5, 0, 0.5 or 1 with -1 the most negative and 1 the most positive:")
        tweet['label'] = float(value)
    elif tweet['extended_fulltext'] is None and tweet['position'] != 'from response_id':
        print(tweet['text'], ";The position of this tweet in the interaction is {}".format(
            tweet['position']))
        value = input("Please type -1, -0.5, 0, 0.5 or 1 with -1 the most negative and 1 the most positive:")
        tweet['label'] = float(value)
    elif tweet['position'] == 'from response_id':
        tweet['label'] = str(None)
print("You're done labelling now! Good Job :)")
# find way to automatically calculate the difference between initial tweet and reaction labels
# for column diff_after_response
for index, tweet in label_df.iterrows():
    if tweet['position'] == 'from reaction_id':
        tweet['diff_after_response'] = tweet['label'] - label_df[(label_df['position'] == 'from tweet_id') & (
                label_df['interaction_nr'] == tweet['interaction_nr'])]['label'].item()
    else:
        tweet['diff_after_response'] = str(None)

# instead of making a new table, make a csv file so only one person has to label the tweets
label_df.to_csv('labels_testset.csv', index=False)
