import logging

import pandas as pd
from tqdm import tqdm

from text_analysis.utils import timeit
from text_analysis.sec_scraper import SECScraper
from text_analysis.sentiment_analyzer import SentimentAnalyzer
from text_analysis.ten_k_extractor import TenKExtractor
from dotenv import load_dotenv
import os

load_dotenv()


def extract_data_bankrupt_companies():
    merged_companies_path = f'{os.getenv("BASE_PATH")}/data/merged_bankrupt_companies.csv'
    df = pd.read_csv(merged_companies_path)
    cik_codes = df['CIK_extracted'].tolist()
    # Extract date for each company and subtract 1 year
    dates = df['YearFiled'].tolist()
    date_range = [(str(int(date) - 1), str(int(date) - 1)) for date in dates]

    df.index = df['CIK_extracted']
    sentiment_results = {str(cik_code): {} for cik_code in cik_codes}

    for i in range(len(cik_codes)):
        cik_code = str(cik_codes[i])
        date = date_range[i]
        extractor = TenKExtractor(cik_code, str(date[0]), str(date[1]))
        ten_k_filings = extractor.get_ten_k_filings()

        for date, document in ten_k_filings.items():
            logging.info(f"Analyzing document for company {i+1}/{len(cik_codes)+1}: {cik_code} on date {date}")
            analyzer = SentimentAnalyzer()
            features = analyzer.analyze_sections(document)
            sentiment_results[cik_code] = features

        if i % 5 == 0:
            results_df = pd.DataFrame(sentiment_results.values(), index=sentiment_results.keys())
            results_df.to_csv(f'{os.getenv("BASE_PATH")}/data/sentiment_results.csv')

    return sentiment_results


def main():
    cik_code = "320193"
    date_range = ('2020', '2020')
    extractor = TenKExtractor(cik_code, date_range[0], date_range[1])
    ten_k_filings = extractor.get_ten_k_filings()

    for date, document in ten_k_filings.items():
        logging.info(f"Analyzing document for company {cik_code} on date {date}")
        analyzer = SentimentAnalyzer()
        features = analyzer.analyze_sections(document)
        logging.info(f"Analysis complete for company {cik_code} on date {date}")

        # Create a DataFrame from the features
        print(features)


if __name__ == '__main__':
    extract_data_bankrupt_companies()
