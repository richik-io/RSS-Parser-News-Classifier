import feedparser
import requests
from bs4 import BeautifulSoup
import pandas as pd

def clean_urls(urls):
    clean_urls = []
    for url in urls:
        try:
            d = feedparser.parse(url)
            if not d.entries:
                raise Exception("No entries found")
            clean_urls.append(url)
            print(f'{url} parsed successfully')
        except Exception as e:
            print(f'{url} couldn’t be parsed. Reason: {e}')
    return clean_urls

def check_duplicates(data):
    # Check for duplicates across all columns
    del data['content']
    df = pd.DataFrame(data)
    duplicates = df[df.duplicated(keep=False)]  # keep=False marks all duplicates (not just the first occurrence)

    if not duplicates.empty:
        print(f"Found {len(duplicates)} duplicate rows:")
        return duplicates
    else:
        print("No duplicates found.")
        return df

def extract_data(data,entry):
    
    '''TITLE'''
    title = getattr(entry, 'title', None)
    data['title'].append(title)

    '''DATE'''
    date = getattr(entry, 'published', None)
    data['date'].append(date)

    '''URL'''
    link = getattr(entry, 'link', 'None')
    data['url'].append(link)

    '''CONTENT'''
    content = getattr(entry, 'content', None)
    if content:
        data['content'].append(content[0].value)
    else:
        result = ''
        try:
            response = requests.get(link)
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, "html.parser")
                paragraphs = soup.find_all('p')  # Use <p> for body content
                result = ' '.join(para.get_text() for para in paragraphs)
                data['content'].append(result)
            else:
                data['content'].append(None)
        except Exception as e:
            print(f"Error fetching content for {link}: {e}")
            data['content'].append(None)
    return None
        

