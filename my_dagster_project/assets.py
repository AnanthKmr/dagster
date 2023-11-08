import json
import os

import requests
from dagster import asset
from newsapi import NewsApiClient

newsapi = NewsApiClient('1febb39041144f5a9ec88e941dac7e1f')
@asset
def topheadlines() -> None:
    top_headlines = newsapi.get_top_headlines(q='bitcoin',
                                          sources='bbc-news,the-verge',
                                          category='business',
                                          language='en',
                                          country='us')
    os.makedirs("data",exist_ok=True)
    with open("data/topheadlines.json","w") as f:
        json.dump(top_headlines,f)

@asset
def allArticles() -> None:
    all_articles = newsapi.get_everything(q='bitcoin',
                                      sources='bbc-news,the-verge',
                                      domains='bbc.co.uk,techcrunch.com',
                                      from_param='2023-11-01',
                                      to='2023-11-07',
                                      language='en',
                                      sort_by='relevancy',
                                      page=2)
    os.makedirs("data",exist_ok=True)
    with open("data/all_articles.json","w") as f:
        json.dump(all_articles,f)
        
@asset
def sources() -> None:
    sources = newsapi.get_sources()
    os.makedirs("data",exist_ok=True)
    with open("data/sources.json","w") as f:
        json.dump(sources,f)

@asset
def topstories() -> None: 
    country = "us"
    newsApiKey = '1febb39041144f5a9ec88e941dac7e1f'
    newsstories_url = f"https://newsapi.org/v2/top-headlines?country={country}&apikey={newsApiKey}"
    top_new_stories = requests.get(newsstories_url).json()

    os.makedirs("data",exist_ok=True)
    with open("data/topstories.json","w") as f:
        json.dump(top_new_stories,f)
