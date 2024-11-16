import logging
import os
import re
from dotenv import load_dotenv
import pandas as pd

from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textstat import flesch_reading_ease
from transformers import pipeline
import torch
import nltk
from text_analysis.utils import timeit

load_dotenv()
lm_dictionary_path = os.getenv("LM_DICTIONARY_PATH")


class SentimentAnalyzer:
    """
    A class to analyze sentiment and extract features from text sections.

    Attributes:
    ----------
    sections : dict
        A dictionary containing text sections to be analyzed.
    tokens : str
        The preprocessed tokens of the text.
    """

    def __init__(self):
        """
        Constructs all the necessary attributes for the SentimentAnalyzer object.

        Parameters:
        ----------
        sections : dict
            Dictionary of section names and text content only.
        """
        self.sections = None
        self.tokens = None
        device = 0 if torch.backends.mps.is_available() else -1  # Use MPS if available
        logging.info(f"Using device {device} for sentiment analysis")
        self.sentiment_pipeline = pipeline(
            'sentiment-analysis',
            model='soleimanian/financial-roberta-large-sentiment',  # FinRoBERTa model for financial text
            device=device
        )
        self.finbert_pipeline = pipeline(
            'sentiment-analysis',
            model='yiyanghkust/finbert-tone',
            device=device
        )
        self.lm_dict = self.load_lm_dictionary()

    def preprocess_text(self, text) -> str:
        """
        Preprocesses the text by removing extra spaces, punctuation, and stopwords, and converting to lowercase.
        """
        text = re.sub(r'\s+', ' ', text)  # Remove extra spaces
        text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
        text = text.lower()  # Convert to lowercase
        stop_words = set(stopwords.words('english'))
        tokens = word_tokenize(text)
        tokens = [word for word in tokens if word not in stop_words]
        self.tokens = ' '.join(tokens)
        return ' '.join(tokens)

    def analyze_finbert(self, text) -> dict:
        """
        Analyzes sentiment using the FinBERT model.
        """
        sentiment = self.finbert_pipeline(text, truncation=True, max_length=512)[0]
        return {"finbert_score": sentiment['score']}

    def analyze_conventional(self, text):
        """
        Analyzes sentiment using FinRoBERTa model.

        Parameters:
        ----------
        text : str
            The preprocessed text.

        Returns:
        -------
        dict
            Sentiment metrics from the conventional model.
        """
        sentiment = self.sentiment_pipeline(text, truncation=True, max_length=512)[0]
        return {"conventional_score": sentiment['score']}

    def load_lm_dictionary(self):
        lm_dict = pd.read_csv(lm_dictionary_path, index_col=0)
        lm_dict.index = lm_dict.index.str.lower()  # Ensure lowercase index for consistency

        # Convert year values to binary indicators
        sentiment_columns = ["Positive", "Negative", "Uncertainty", "Litigious", "Constraining", "Strong_Modal",
                             "Weak_Modal"]
        for col in sentiment_columns:
            if col in lm_dict.columns:
                lm_dict[col] = lm_dict[col].apply(lambda x: 1 if x != 0 else 0)
        lm_dict.columns = lm_dict.columns.str.lower()
        return lm_dict

    def analyze_loughran_mcdonald(self, text):
        tokens = word_tokenize(text)
        token_counts = Counter(tokens)

        scores = {
            "positive": 0,
            "negative": 0,
            "uncertainty": 0,
            "litigious": 0,
            "constraining": 0,
            "strong_modal": 0,
            "weak_modal": 0
        }

        for word, count in token_counts.items():
            word = word.lower()
            if word in self.lm_dict.index:
                for key in scores.keys():
                    try:
                        # Safely add scores
                        scores[key] += self.lm_dict.at[word, key] * count
                    except KeyError as e:
                        logging.warning(f"KeyError: {e} for word '{word}' in column '{key}'")
                        continue  # Skip problematic words or keys

        # convert to float
        for key in scores.keys():
            scores[key] = float(scores[key])

        return scores

    def extract_text_metrics(self, text):
        """
        Extracts text-related metrics.

        Parameters:
        ----------
        text : str
            The preprocessed text.

        Returns:
        -------
        dict
            Text metrics such as word count, average word length, and reading ease.
        """
        word_count = len(text.split())
        char_count = len(text)
        avg_word_length = char_count / word_count if word_count > 0 else 0
        reading_ease = flesch_reading_ease(text) if word_count > 0 else 0

        return {
            "word_count": word_count,
            "avg_word_length": avg_word_length,
            "reading_ease": reading_ease
        }

    def process_chunk(self, chunk):
        truncated_text = ' '.join(chunk)

        # Collect all metrics
        text_metrics = self.extract_text_metrics(truncated_text)
        finbert_metrics = self.analyze_finbert(truncated_text)
        conventional_metrics = self.analyze_conventional(truncated_text)
        lm_metrics = self.analyze_loughran_mcdonald(truncated_text)

        return text_metrics, finbert_metrics, lm_metrics, conventional_metrics

    def process_metrics(self, chunks, preprocessed_text, results):
        # Initialize metrics accumulators
        aggregated_metrics = {
            "word_count": 0,
            "avg_word_length": 0,
            "reading_ease": 0,
            "finbert_score": 0,
            "polarity_score": 0,
            "positive": 0,
            "negative": 0,
            "uncertainty": 0,
            "litigious": 0,
            "constraining": 0,
            "strong_modal": 0,
            "weak_modal": 0
        }

        for text_metrics, finbert_metrics, lm_metrics, conventional_metrics in results:
            # Aggregate metrics
            aggregated_metrics["word_count"] += text_metrics["word_count"]
            aggregated_metrics["avg_word_length"] += text_metrics["avg_word_length"]
            aggregated_metrics["reading_ease"] += text_metrics["reading_ease"]
            aggregated_metrics["finbert_score"] += finbert_metrics["finbert_score"]
            aggregated_metrics["polarity_score"] += conventional_metrics['conventional_score']
            aggregated_metrics["positive"] += lm_metrics["positive"]
            aggregated_metrics["negative"] += lm_metrics["negative"]
            aggregated_metrics["uncertainty"] += lm_metrics["uncertainty"]
            aggregated_metrics["litigious"] += lm_metrics["litigious"]
            aggregated_metrics["constraining"] += lm_metrics["constraining"]
            aggregated_metrics["strong_modal"] += lm_metrics["strong_modal"]
            aggregated_metrics["weak_modal"] += lm_metrics["weak_modal"]

        # Average out the metrics that need averaging
        num_chunks = len(chunks)
        aggregated_metrics["avg_word_length"] /= num_chunks
        aggregated_metrics["reading_ease"] /= num_chunks
        aggregated_metrics["finbert_score"] /= num_chunks
        aggregated_metrics["polarity_score"] /= num_chunks

        num_words = len(preprocessed_text.split())
        aggregated_metrics["positive"] /= num_words
        aggregated_metrics["negative"] /= num_words
        aggregated_metrics["uncertainty"] /= num_words
        aggregated_metrics["litigious"] /= num_words
        aggregated_metrics["constraining"] /= num_words
        aggregated_metrics["strong_modal"] /= num_words
        aggregated_metrics["weak_modal"] /= num_words

        return aggregated_metrics

    @timeit
    def analyze_sections(self, sections):
        """
        Analyzes the sentiment and text metrics for each section.

        Parameters:
        ----------
        sections : dict
            A dictionary containing text sections to be analyzed.

        Returns:
        -------
        list
            A list of dictionaries containing the extracted features for each section.
        """
        features = []
        max_length = 512

        for section_name, text in sections.items():
            if not text:
                continue
            preprocessed_text = self.preprocess_text(text)

            # Split text into chunks of 512 tokens
            tokens = word_tokenize(preprocessed_text)
            chunks = [tokens[i:i + max_length] for i in range(0, len(tokens), max_length)]
            # Ensure no token exceeds the max_length limit
            chunks = [token[:512] if len(token) > 512 else token for token in chunks]

            if len(chunks) == 0 or not chunks:
                continue
            results = [self.process_chunk(chunk) for chunk in chunks]
            # with concurrent.futures.ThreadPoolExecutor() as executor:
            #     results = list(executor.map(self.process_chunk, chunks))
            aggregated_metrics = self.process_metrics(chunks, preprocessed_text, results)
            # Combine all metrics
            section_features = {
                "section": section_name,
                **aggregated_metrics
            }
            features.append(section_features)

        all_features = {}
        for feature in features:
            # Create a new dictionary with modified keys
            new_feature = {f"{feature['section']}_{key}": value for key, value in feature.items()}
            # Update the all_features dictionary with the new_feature dictionary
            all_features.update(new_feature)

        return all_features


if __name__ == '__main__':
    # Download resources for NLTK
    nltk.download('stopwords')
    nltk.download('punkt')

    sections = {
        "Item1": "This section discusses the risks associated with our business...",
        "Item1A": "The management discusses financial results and future outlook...",
        "Item7": "Our consolidated financial statements present a snapshot of..."
    }

    analyzer = SentimentAnalyzer(sections)
    features = analyzer.analyze_sections(sections)

    print(features)
