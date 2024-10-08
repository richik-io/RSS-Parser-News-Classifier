from extract import clean_urls, extract_data, check_duplicates
from load import store_data_in_db
from classifier import classify
import pandas as pd
import feedparser

urls = [
    'http://rss.cnn.com/rss/cnn_topstories.rss',
    'http://qz.com/feed',
    'http://feeds.foxnews.com/foxnews/politics',
    'http://feeds.reuters.com/reuters/businessNews',
    'http://feeds.feedburner.com/NewshourWorld',
    'https://feeds.bbci.co.uk/news/world/asia/india/rss.xml'
]
urls = clean_urls(urls)
data = {'title': [], 'content': [], 'date': [], 'url': [],'pred':[]}
# index=0
for url in urls:
    print(f"Started parsing {url}")
    d = feedparser.parse(url)
    for i in range(len(d.entries)):
        extract_data(data,d.entries[i])
        pred = classify(data['title'][i])
        data['pred'].append(pred['labels'][0])
        print(data['pred'][i])
df =  check_duplicates(data)
# Store as csv
df.to_csv('data.csv')
    # store_data_in_db(df.loc[index:])
    # index = df.shape[0]
