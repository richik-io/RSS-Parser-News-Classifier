from celery_config import app
from extract import extract_data, check_duplicates
from classifier import classify
from load import store_data_in_db

# Celery task for extracting data
@app.task
def extract_data_task(data, entry):
    """Celery task to extract data from an RSS feed entry."""
    extract_data(data, entry)  # Use the extract_data function to populate `data`
    return data

# Celery task for classifying the article title
@app.task
def classify_data_task(data):
    """Celery task to classify the extracted data."""
    for i in range(len(data['title'])):
        pred = classify(data['title'][i])  # Use the classifier on each title
        data['pred'].append(pred)
    return data

# Celery task for storing data into the database
@app.task
def store_data_task(data, start_index):
    """Celery task to store the data into the database."""
    df = check_duplicates(data)
    store_data_in_db(df.loc[start_index:])  # Store only new entries based on the index
    return df.shape[0]  # Return the updated index
