import pandas as pd
from tqdm import tqdm

from text_analysis.utils import timeit
from text_analysis.sec_scraper import SECScraper
from text_analysis.sentiment_analyzer import SentimentAnalyzer
from text_analysis.ten_k_extractor import TenKExtractor


@timeit
def download_cik_codes():
    """
    Downloads the CIK codes for all companies in the S&P 500 index.
    :return:
    """
    file_path = "/Users/andreadonazzan/Downloads/Export 15_11_2024 15_18.csv"
    scraper = SECScraper()
    df = pd.read_csv(file_path)
    df.index = df['Ragione socialeCaratteri latini']
    company_names = df.index.tolist()
    cik_codes = {company_names[i]: None for i in range(len(company_names))}
    for i in tqdm(range(len(company_names)), desc="Downloading CIK codes"):
        cik_code = scraper.lookup_company_name(company_names[i])
        cik_codes[company_names[i]] = cik_code
        if i % 100 == 0:
            df['CIK_extracted'] = cik_codes
            df.to_csv(file_path)

    df['CIK_extracted'] = cik_codes
    df.to_csv(file_path)


def main():
    cik_code = "320193"
    date_range = ('2020', '2020')
    extractor = TenKExtractor(cik_code, date_range[0], date_range[1])
    ten_k_filings = extractor.get_ten_k_filings()

    for date, document in ten_k_filings.items():
        analyzer = SentimentAnalyzer()
        features = analyzer.analyze_sections(document)
        print(features)

        # Create a DataFrame from the features
        features_df = pd.DataFrame(features)
        print(features_df)
        print(features_df.describe())
        features_df.to_csv(f"{cik_code}_{date}.csv")


if __name__ == '__main__':
    download_cik_codes()
