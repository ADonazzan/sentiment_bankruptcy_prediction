import pandas as pd
from tqdm import tqdm

from text_analysis.utils import timeit
from text_analysis.sec_scraper import SECScraper
from text_analysis.sentiment_analyzer import SentimentAnalyzer
from text_analysis.ten_k_extractor import TenKExtractor
from dotenv import load_dotenv
import os

load_dotenv()


@timeit
def download_cik_codes():
    """
    Downloads the CIK codes for all companies who have gone bankrupt.
    :return:
    """
    bc_names_path = f'{os.getenv("BASE_PATH")}/data/bankrupt_companies_names.csv'
    bc_data_path = f'{os.getenv("BASE_PATH")}/data/bankrupt_companies_data.csv'
    scraper = SECScraper()
    df_names = pd.read_csv(bc_names_path)
    df_data = pd.read_csv(bc_data_path)
    company_names = df_names['NameCorp'].tolist()
    cik_codes = []
    for company in tqdm(company_names):
        cik_code = scraper.lookup_company_name(company)
        if cik_code == 'Perform another Company-CIK Lookup.':
            cik_code = None
        cik_codes.append(cik_code)
    df_names['CIK_extracted'] = cik_codes
    df_names.to_csv(bc_names_path)

    company_names = df_data['Ragione sociale'].tolist()
    cik_codes = []
    for company in tqdm(company_names):
        cik_code = scraper.lookup_company_name(company)
        if cik_code == 'Perform another Company-CIK Lookup.':
            cik_code = None
        cik_codes.append(cik_code)
    df_data['CIK_extracted'] = cik_codes
    df_data.to_csv(bc_data_path)

    return df_names, df_data


def merge_tables(df1, df2):
    """
    Merges two dataframes on the CIK_extracted column.
    """
    # delete rows with none cik_code or 'Perform another Company-CIK Lookup
    df1 = df1[df1['CIK_extracted'].notna()]
    df1 = df1[df1['CIK_extracted'] != 'Perform another Company-CIK Lookup.']
    df2 = df2[df2['CIK_extracted'].notna()]
    df2 = df2[df2['CIK_extracted'] != 'Perform another Company-CIK Lookup.']
    merged_df = pd.merge(df1, df2, on='CIK_extracted', how='inner')
    return merged_df


if __name__ == "__main__":
    # df_names, df_data = download_cik_codes()
    df_names = pd.read_csv(f'{os.getenv("BASE_PATH")}/data/bankrupt_companies_names.csv')
    df_data = pd.read_csv(f'{os.getenv("BASE_PATH")}/data/bankrupt_companies_data.csv')
    merged_df = merge_tables(df_names, df_data)
    merged_df.to_csv(f'{os.getenv("BASE_PATH")}/data/merged_bankrupt_companies.csv', index=False)
