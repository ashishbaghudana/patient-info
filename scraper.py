from bs4 import BeautifulSoup
from typing import List

import requests
import os


URLs = List[str]


def scrape(urls: URLs, file_path: str='/Users/ashish/code/patientinfo/data'):
    request_list = []
    if not os.path.exists(file_path) and not os.path.isdir(file_path):
        os.mkdir(file_path)
    elif os.path.isdir(file_path):
        pass
    else:
        print('Can\'t create directory. Exiting.')
        exit(1)
    for link in urls:
        print(f'Scraping for link: {link}')
        urls = get_forums_in_url(link)
        for url in urls:
            request_list.extend(scrape_forum(url, file_path))
    print(request_list)
    return request_list


def scrape_forum(link: str, file_path: str, prefix: str='https://patient.info') -> URLs:
    urls = []
    page_id = 0
    directory = os.path.join(file_path, link.split('/')[-1])
    if not os.path.exists(directory) and not os.path.isdir(directory):
        os.mkdir(directory)
    print(f'Scraping page %s%s?page=%d' % (prefix, link, page_id))
    response = requests.get('%s%s?page=%d' % (prefix, link, page_id))
    while response.ok:
        soup = BeautifulSoup(response.text, 'html.parser')
        for href in soup.find_all('a', class_='thread-ctrl'):
            if href.attrs['href'].startswith('/'):
                urls.append(href.attrs['href'])
        page_id += 1
        print(f'Scraping page %s%s?page=%d' % (prefix, link, page_id))
        response = requests.get('%s%s?page=%d' % (prefix, link, page_id), timeout=10)
    for url in urls:
        scrape_thread(url, directory)
    return urls


def scrape_thread(link: str, directory: str, prefix: str='https://patient.info'):
    print(f'Downloading HTML content for {prefix}{link}')
    if os.path.exists('%s.html' % os.path.join(directory, link.split('/')[-1])):
        return
    response = requests.get(f'{prefix}{link}', timeout=10)
    with open('%s.html' % os.path.join(directory, link.split('/')[-1]), 'w') as fwriter:
        fwriter.write(response.text)


def get_forums_in_url(link: str) -> URLs:
    urls = []
    print(f'Scraping page %s' % link)
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    for td in soup.findChildren('td'):
        link = td.findChild('a').attrs['href']
        if link is not None:
            urls.append(link)
    return urls


def main():
    letters = set('a')
    urls = ['https://patient.info/forums/index-%s' % letter for letter in letters]
    scrape(urls)


if __name__ == '__main__':
    main()
