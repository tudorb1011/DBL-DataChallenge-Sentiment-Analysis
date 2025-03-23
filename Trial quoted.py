database = sqlite3.connect('temporary_database.sqlite3')

query_all = 'SELECT * FROM TWEETS'

df_tweet = pd.read_sql_query(query_all, database)
quote_tweet = pd.DataFrame()
quote_tweet = df_tweet[(df_tweet['lang'] == 'en') & (~df_tweet['quoted_status_id'].isna())]
quote_tweet_new = quote_tweet.set_index('user_id')
dicti : dict = dict()
# quote_tweet_new
for user in quote_tweet_new.index:
    dicti['citat'] = quote_tweet_new['text'].str.split('@')
print(dicti)