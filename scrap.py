import os
import sys
import argparse
import locale
import time
from datetime import datetime
# import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen


locale.setlocale(locale.LC_ALL, 'id_ID.utf-8')

os.environ['TZ'] = 'Asia/Jakarta'
time.tzset()

data = {}

def scrap(url):
    resp = urlopen(url)
    # print(resp.read())
    soup = BeautifulSoup(resp.read())
    
    konten = []
    contents = soup.findAll('span', {'class': 'Textweb__StyledText-sc-2upo8d-0 hmCMdT'})
    for content in contents:
        konten.append(content.get_text())

    date = soup.find('span', {'class': 'Textweb__StyledText-sc-2upo8d-0 bcNzUB'}).get_text()
    ndate = datetime.strptime(date, '%d %B %Y %H:%M')

    data['author'] = soup.find('span', {'class': 'Textweb__StyledText-sc-2upo8d-0 cplyL'}).get_text()
    data['title'] = soup.find('span', {'class': 'Textweb__StyledText-sc-2upo8d-0 OGIgR'}).get_text()
    data['content'] = ''.join(str(content) for content in konten)
    data['image_url'] = soup.find('div',{'class': 'Helper__MediaWrapper-sc-1cebgcc-10 hvfAUV'}).img['src']
    data['date'] = ndate.date().strftime('%Y-%m-%d')
    
    return data

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--url')
    args = parser.parse_args()
    
    url = args.url
    print(scrap(url))