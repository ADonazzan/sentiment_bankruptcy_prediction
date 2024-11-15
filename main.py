import pandas as pd

from text_analysis.sentiment_analyzer import SentimentAnalyzer
from text_analysis.ten_k_extractor import TenKExtractor

if __name__ == '__main__':
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


