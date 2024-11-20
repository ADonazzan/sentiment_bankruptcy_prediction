import logging

import pandas as pd
from tqdm import tqdm

from text_analysis.utils import timeit
from text_analysis.sec_scraper import SECScraper
from text_analysis.sentiment_analyzer import SentimentAnalyzer
from text_analysis.ten_k_extractor import TenKExtractor
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()


def extract_data_companies(file_name: str, excluded_companies_file_names: str = None):
    start_time = datetime.now()
    merged_companies_path = f'{os.getenv("BASE_PATH")}/data/{file_name}'
    df = pd.read_csv(merged_companies_path, na_values=['n.d.', 'n.s.'])
    df = df[df['CIK_extracted'].notna()]
    df['CIK_extracted'] = df['CIK_extracted'].astype(int).astype(str)
    # Sort the DataFrame by the number of NaN values in each row (ascending)
    df['na_count'] = df.isna().sum(axis=1)
    df = df.sort_values(by=['na_count'], ascending=True).drop(columns='na_count')
    # remove duplicates
    df.drop_duplicates(subset=['CIK_extracted', 'year'], inplace=True)

    df['run_analysis'] = True
    df['year_before'] = df['Ultimo anno disp.'] - 1

    if excluded_companies_file_names:
        excluded_companies = pd.read_csv(f'{os.getenv("BASE_PATH")}/data/{excluded_companies_file_names}')
        excluded_companies = excluded_companies['cik_code'].astype(int).astype(str).tolist()
        df.loc[df['CIK_extracted'].isin(excluded_companies), 'run_analysis'] = False
        # df.loc[df['Utile netto migl USD'] == 'n.d.', 'run_analysis'] = False
        df.drop(columns=['year_before'], inplace=True)

    df = df[df['run_analysis'] == True]
    df.set_index(['CIK_extracted', 'year'], inplace=True)
    # Convert cik codes to string with no decimal points
    df.index = df.index.map(lambda x: (str(int(x[0])), x[1]))
    df = df[~df.index.duplicated(keep='first')]

    codes_years = df.index.tolist()
    cik_codes = [code[0] for code in codes_years]
    # remove duplicates
    cik_codes = list(set(cik_codes))
    years = df.index.get_level_values('year').unique().tolist()

    chunks = [cik_codes[i:i + 20] for i in range(0, len(cik_codes), 20)]
    for j, chunk in enumerate(chunks):
        sentiment_results = []
        df = []
        for i, cik_code in enumerate(chunk):
            cik_code = str(cik_code)
            for year in years:
                extractor = TenKExtractor(cik_code, str(year), str(year))
                ten_k_filings = extractor.get_ten_k_filings()

                for date, document in ten_k_filings.items():
                    logging.info(
                        f"Analyzing document for company {i + j * 20}/{len(cik_codes) + 1}: {cik_code} on date {date}")
                    analyzer = SentimentAnalyzer()
                    features = analyzer.analyze_sections(document)
                    features['cik_code'] = cik_code
                    features['year'] = year
                    features['date'] = date
                    sentiment_results.append(features)

                if i % 1 == 0 and i != 0:
                    results_df = pd.DataFrame(sentiment_results)
                    try:
                        results_df.set_index(['cik_code', 'year', 'date'], inplace=True)
                    except:
                        pass
                    # save file with starting minute
                    results_df.to_csv(
                        f'{os.getenv("BASE_PATH")}/data/sentiment_results/{file_name[:-4]}_{start_time.day}_'
                        f'{start_time.hour}_{start_time.minute}_{j}.csv', index=True)

        results_df = pd.DataFrame(sentiment_results)
        results_df.to_csv(
            f'{os.getenv("BASE_PATH")}/data/sentiment_results/{file_name[:-4]}_{start_time.day}_{start_time.hour}'
            f'_{start_time.minute}_{j}.csv', index=True)

        # Clear sentiment_results to free up memory
        sentiment_results = []

    return sentiment_results


def download_cik_codes():
    """
    Downloads the CIK codes for all companies-
    :return:
    """
    data_path = f'{os.getenv("BASE_PATH")}/data/merged_data.csv'
    scraper = SECScraper()
    df_data = pd.read_csv(data_path)

    company_names = df_data['Ragione sociale'].tolist()
    cik_codes = []
    for company in tqdm(company_names):
        cik_code = scraper.lookup_company_name(company)
        if cik_code == 'Perform another Company-CIK Lookup.':
            cik_code = None
        cik_codes.append(cik_code)
    df_data['CIK_extracted'] = cik_codes
    df_data.to_csv(data_path)

    return df_data


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
    extract_data_companies('transformed_companies.csv', 'already_analyzed.csv')
