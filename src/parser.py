from tasks import extract_data_task, classify_data_task, store_data_task
import feedparser

urls = [
    'http://rss.cnn.com/rss/cnn_topstories.rss',
    'http://qz.com/feed',
    'http://feeds.foxnews.com/foxnews/politics',
    'http://feeds.reuters.com/reuters/businessNews',
    'http://feeds.feedburner.com/NewshourWorld',
    'https://feeds.bbci.co.uk/news/world/asia/india/rss.xml'
]

data = {'title': [], 'content': [], 'date': [], 'url': [], 'pred': []}
index = 0  # Track the index to avoid re-storing duplicates

for url in urls:
    # Parse the RSS feed
    d = feedparser.parse(url)
    
    # Loop through each article in the feed
    for entry in d.entries:
        # Step 1: Asynchronously extract data from the article entry
        extract_task = extract_data_task.s(data, entry)
        
        # Step 2: Asynchronously classify the extracted data after it's done
        classify_task = classify_data_task.s(data)
        
        # Step 3: Asynchronously store the classified data into the database
        store_task = store_data_task.s(data,index)
        
        # Chain the tasks: extract -> classify -> store
        chain = (extract_task | classify_task | store_task)
        
        # Apply the task chain
        chain.apply_async()

        # Update the index after storing data to ensure no duplicates are processed
        index = store_task.result or index
