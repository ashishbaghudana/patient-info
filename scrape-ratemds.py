from retrying import retry

import json
import requests
import os


def scrape(url: str='https://www.ratemds.com/api/doctor/',
           file_path: str='/Users/ashish/code/scrape-medical-data/data/ratemds'):
    if not os.path.exists(file_path) and not os.path.isdir(file_path):
        os.mkdir(file_path)
    elif os.path.isdir(file_path):
        pass
    else:
        print('Can\'t create directory. Exiting.')
        exit(1)
    index = 1
    while index < 2150000:
        response = make_request(f'{url}{index}/')
        if response.ok:
            with open(os.path.join(file_path, f'{index}.json'), 'w') as fwriter:
                fwriter.write(json.dumps(response.json()))
        index += 1


@retry(stop_max_attempt_number=100, wait_random_min=1000, wait_random_max=2000)
def make_request(url: str) -> requests.Response:
    response = requests.get(url)
    return response


def main():
    scrape()


if __name__ == '__main__':
    main()
