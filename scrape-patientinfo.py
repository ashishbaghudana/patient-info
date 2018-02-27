from bs4 import BeautifulSoup
from typing import List
from retrying import retry
from multiprocessing.dummy import Pool as ThreadPool

import requests
import os


URLs = List[str]


def scrape(urls: URLs, file_path: str='/Users/ashish/code/scrape-medical-data/data'):
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


@retry(stop_max_attempt_number=3)
def scrape_forum(link: str, file_path: str, prefix: str='https://patient.info') -> URLs:
    urls = []
    page_id = 0
    directory = os.path.join(file_path, link.split('/')[-1])
    if not os.path.exists(directory) and not os.path.isdir(directory):
        os.mkdir(directory)
    print(f'Scraping page %s%s?page=%d' % (prefix, link, page_id))
    response = requests.get('%s%s' % (prefix, link))
    if response.url != link:
        link = response.url
        prefix = ''
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


@retry(stop_max_attempt_number=3)
def scrape_thread(link: str, directory: str, prefix: str='https://patient.info'):
    print(f'Downloading HTML content for {prefix}{link}')
    if os.path.exists('%s.html' % os.path.join(directory, link.split('/')[-1])):
        return
    response = requests.get(f'{prefix}{link}', timeout=10)
    with open('%s.html' % os.path.join(directory, link.split('/')[-1]), 'w') as fwriter:
        fwriter.write(response.text)


def get_forums_in_url(link: str, prefix: str='https://patient.info') -> URLs:
    urls = []
    print(f'Scraping page %s' % link)
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    for td in soup.findChildren('td'):
        link = td.findChild('a').attrs['href']
        if link is not None:
            urls.append(f'{prefix}{link}')
    return urls


def num_collections(urls: URLs) -> int:
    count = 0
    for link in urls:
        print(f'Scraping for link: {link}')
        links = get_forums_in_url(link)
        count += len(links)
    return count


def _get_discussions(link: str):
    discussion_list = []
    print(f'Scraping for link: {link}')
    forums = get_forums_in_url(link)
    for url in forums:
        page_id = 0
        print(f'Scraping forum {url}')
        response = requests.get(url)
        while response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')
            for href in soup.find_all('a', class_='thread-ctrl'):
                if href.attrs['href'].startswith('/'):
                    discussion_list.append(href.attrs['href'])
            page_id += 1
            print(f'Scraping page %s?page=%d' % (url, page_id))
            response = requests.get('%s?page=%d' % (url, page_id), timeout=20)
    return discussion_list


def get_all_discussions(urls: URLs):
    pool = ThreadPool(4)
    discussion_list = pool.map(_get_discussions, urls)
    return discussion_list


def main():
    with open('forums-patientinfo.cfg') as freader:
        letters = freader.readline().strip()
        data_dir = freader.readline().split()
    letters = set(letters)
    urls = ['https://patient.info/forums/index-%s' % letter for letter in letters]
    scrape(urls, data_dir[0])


def count():
    with open('forums-patientinfo.cfg') as freader:
        letters = freader.readline().strip()
    letters = set(letters)
    urls = ['https://patient.info/forums/index-%s' % letter for letter in letters]
    print(num_collections(urls))


if __name__ == '__main__':
    with open('forums-patientinfo.cfg') as freader:
        letters = freader.readline().strip()
    letters = set(letters)
    urls = ['https://patient.info/forums/index-%s' % letter for letter in letters]
    get_all_discussions(urls)
