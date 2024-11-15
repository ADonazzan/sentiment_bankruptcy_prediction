import random
import string
import sys
from retrying import retry, RetryError
import requests
import logging
import os
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from text_analysis.utils import rate_limiter

load_dotenv()
logging.basicConfig(level=logging.INFO)


def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_cookies_submissions():
    cookies = {
        '_ga': f'GA1.1.{random.randint(1000000000, 9999999999)}.{random.randint(1000000000, 9999999999)}',
        'nmstat': f'{generate_random_string(8)}-{generate_random_string(4)}-{generate_random_string(4)}-{generate_random_string(4)}-{generate_random_string(12)}',
        'bm_mi': f'{generate_random_string(32)}~YAAQXH4ZuHrxSPuSAQAAGI/1JRmvhG2LUufnIkyCM8VSK+EqCecnOkvQ99vEtLC8tgM9nL0JOxnjdrQ+/KpUdjjVcnDU1VYSZ65TeWP3mqEbGkKAFnF2BrXVYWzZDIi+DQx46nMuBYiy0rsGdRVn9NjoMa/sw+uI11i5IStdCTaKcLaWk2PJNpzEsdU61V5Vd1rcT7L+yN9nf34FQ/3KMFQ+QWTfI7mWsGgJ4s/GmTJac0Agwltk0jeMd81cC6IFVWRKsFlHYSVGQVIHvaYPHe3EPNX/8uEYlTW+zF0POboQ/+z52RBKk/T6Y0vjV8C1SMhvdp3i~1',
        'ak_bmsc': f'{generate_random_string(32)}~000000000000000000000000000000~YAAQXH4ZuOnzSPuSAQAAiZT1JRlxsxbOKDl9MNCAa1sccpwwklOHAQZNtje54qGlkIfJEkzyfLA/OCq4zIZV9Aj7npZiCDqzX9q+s6xTe16x8z4m4528bd11FhPHpxqX7r0GDx+SncVb845CJZP9iAEAfp1cXzKPPgQ/LVbu+GC9mNaSncCV45UlSmbOBjnaaHeTccskzmRHHKvKEwem+jsLarygFMiuTqgsuXhoeora4YPUcxqvyp5rmKitysaL930+jC/SXDl5No248LnIDkNjX4Cyp6LlOjLhHR/mHVZN4t8Y/4Ihn7BKPc3Ke2hzQhGHZ+YgGkbfuxWLDahko6m7EluD1Ldk7sxMT7hZseAGNaq2d5L4FqQcGGGogRy7bB41GFzy5Gh8Ixr32gMir1btqCuFTXoqzxnq8u85F5eHro9ppBOkmgSzYqalBl85zCfGrDqdv4Bq6bhibp4Tz+a64mtEDjF2qDZ6Vo2CKJ73A/kV7Sa5QQ==',
        '_4c_': '%7B%22_4c_s_%22%3A%22lVNNj5swEP0rkc9xYhMDJtec06rVqteVY4%2FBCsHIeEO3Uf57x4TsR9oeyoXxmzfPw5vhQsYGOrLl5YbnfMMKzvlmSY7wOpDthQRn0utMtsTmFjTXmpbGVlQcdE6lqizNNOSqhNwqW5Il%2BTlpVVlRyDLLWHldEt3PGheivQHU4tWKyxWjdsCK%2BAuRgmHUB29edHyOr31ijXBYDOaICQNnp%2BF5dCY2qVwI9o424OomIlyxCe0DxhlGo%2BuMHx%2BrZvStSvLEPQQ%2FDpAqd03wJ1igFwh7dIHslcYwgIUQJkoTYz9s1%2BtxHFe193ULK%2B1PayQNLqbOB9CYOM8AmnnD6A374RJqFk%2B774h%2F%2BYB8233dz1CfPBcYtF6rNonimJbkJbQPDcx3rQdQQTfUutZ19bAGU6tAVd%2B3TqvofEfR3Tqo0wnT1HURglUa0gCgS3f1waR%2BQSfyrV88PwVX1xD2EBuPq4BnZVxiqDZ5nYgBDAyuTt2ZZCiejtH3b%2FD1vhQ8ryqRVzzLcOgRv0MWgqXnevvcaUfEB3bOuKwy%2Fif7Ni2aLIXu%2F0oN2PtGS1AbYcuCcgaSilJktCqLDWWHAmQpDNNckc%2BaFeN%2F0%2Bzukh%2B8uP8IUhSprECe6%2BNMnASZxAy2%2BcBFJHHvkupR65YP47vUlMiYkPITdUKSt%2BHh1n9Rr9ff%22%7D',
        'bm_sv': f'{generate_random_string(32)}~YAAQXH4ZuHSVavuSAQAAYOs4JhnB2A8bLLRqHMku8hTVsxK68VHB9R3HSJjzCcoofn2xyA+t+2v6FEQZeSwuQwQMtnIPXQigPHvZAL8d8ra4czargHxM0CHHXvHI4/0bp/TOert9Z+QCVCKFftQNUCS/ly/YRSWTvRMn+oXoEo8dq+09fWYyYDT1E950RA1SnPV9GXgc1M2kgFR/VFEgRZXo4euZfofVptD3lcGXie7tdxQYJIEk5CKgwWF1bw==~1',
        '_ga_300V1CHKH1': f'GS1.1.{random.randint(1000000000, 9999999999)}.3.1.{random.randint(1000000000, 9999999999)}.0.0.0',
        '_ga_CSLL4ZEK4L': f'GS1.1.{random.randint(1000000000, 9999999999)}.3.1.{random.randint(1000000000, 9999999999)}.0.0.0',
    }
    return cookies


def generate_headers():
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,it;q=0.7',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': os.getenv("USER_AGENT")
    }
    return headers


class SECScraper:
    def __init__(self):
        self.headers = generate_headers()
        self.cookies = generate_cookies_submissions()
        self.base_url = 'https://data.sec.gov'
        self.cik_code = None


    def setup_request(self, endpoint):
        case = endpoint.split('/')[-1]
        if case == 'submissions':
            cookies = generate_cookies_submissions()
            headers = self.headers
        else:
            cookies = self.cookies
            headers = self.headers
        return cookies, headers

    @retry(stop_max_attempt_number=3, wait_fixed=1000)
    def _lookup_company_name(self, company_name):
        url = 'https://www.sec.gov/cgi-bin/cik_lookup'
        data = {'company': company_name}
        cookies, headers = self.setup_request('cik_lookup')
        response = requests.post(url, cookies=cookies, headers=headers, data=data)
        if response.status_code != 200:
            raise RetryError(f"Error in company {company_name}: {response.status_code}")
        return response.text

    @rate_limiter(10)
    def lookup_company_name(self, company_name):
        # check if company has ending /DE/ or /TX/ etc. and remove it
        expression = r'(.+)(/[A-Z]{2,3}/?|(US MARKET AGGREGATE)|(THE)|,|\.)'
        match = re.match(expression, company_name)
        if match:
            company_name = match.group(1)
            # logging.info(f"Removed ending from company name: {company_name}")

        try:
            response = self._lookup_company_name(company_name)
            response_cleaned = BeautifulSoup(response, 'lxml')
            cik_code = response_cleaned.find('a').text
            cik_code_strip_zeros = cik_code.lstrip('0')
            return cik_code_strip_zeros
        except RetryError as e:
            logging.error(f"Failed to fetch data for company {company_name} after 3 retries: {e}")
            return None

    @retry(stop_max_attempt_number=3, wait_fixed=1000)
    def _download_submissions_response(self, endpoint):
        # add initial zeros to the cik_code to make it 10 characters long
        cik_code_long = self.cik_code.zfill(10)
        url = f'{self.base_url}/{endpoint}/CIK{cik_code_long}.json'
        cookies, headers = self.setup_request(endpoint)
        response = requests.get(url, cookies=cookies, headers=headers)
        # Check if the response is valid
        if response.status_code != 200:
            raise RetryError(f"Error in company {self.cik_code}: {response.status_code}")
        response_size = sys.getsizeof(response.content)
        logging.info(f"Downloaded {response_size/1024:.2f} KB for company {self.cik_code}")
        return response.json()

    def get_submissions(self, endpoint="submissions"):
        """
        Extracts all submissions for a given company
        :param endpoint:
        :return:
        """
        try:
            response = self._download_submissions_response(endpoint)
            # Extract the 'filings' -> 'recent' data
            submissions = response['filings']['recent']
            return submissions
        except RetryError as e:
            logging.error(f"Failed to fetch data for company {self.cik_code} after 3 retries: {e}")
            return None

    @retry(stop_max_attempt_number=3, wait_fixed=1000)
    def _download_10k_response(self, endpoint):
        url = f'https://www.sec.gov/{endpoint}'
        cookies, headers = self.setup_request(endpoint)
        response = requests.get(url, cookies=cookies, headers=headers)

        if response.status_code != 200:
            raise RetryError(f"Error in company {self.cik_code}: {response.status_code}")
        return response.text

    def get_10_k_descriptions(self, cik_code, date_start, date_end):
        """
        Extracts only the 10-K descriptions for a given company, within a given date range
        :param cik_code:
        :return:
        """

        self.cik_code = cik_code
        submissions = self.get_submissions()

        if submissions is None:
            return None

        ten_k_filings = [
            {'accessionNumber': submissions['accessionNumber'][i],
             'filingDate': submissions['filingDate'][i],
             'primaryDocument': submissions['primaryDocument'][i],
             'primaryDocDescription': submissions['primaryDocDescription'][i]}
            for i in range(len(submissions['form']))
            if submissions['form'][i].lower() == '10-k'
               and date_start <= submissions['filingDate'][i] <= date_end]

        return ten_k_filings

    def download_10k(self, cik_code, accession_number):
        self.cik_code = cik_code
        endpoint = f'/Archives/edgar/data/{cik_code}/{accession_number.replace('-', '')}/{accession_number}.txt'
        try:
            response = self._download_10k_response(endpoint)
            return response
        except RetryError as e:
            logging.error(f"Failed to fetch data for company {cik_code} after 3 retries: {e}")
            return None


if __name__ == '__main__':
    # Example of extracting 10-K descriptions
    # cik_code = "320193"
    # print(SECScraper().get_10_k_descriptions(cik_code, '2020-01-01', '2030-12-31'))

    # Example of downloading a 10-K filing
    # cik_code = "320193"
    # accession_number = "000032019324000123"
    # primary_document = "aapl-20240928.htm"
    # print(SECScraper().download_10k(cik_code, accession_number, primary_document))

    # Example of looking up a company name
    company_name = 'Walmart inc.'
    print(SECScraper().lookup_company_name(company_name))
