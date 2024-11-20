import os
import pandas as pd
from typing import List
from dotenv import load_dotenv
import logging

load_dotenv()


def merge_csv_files(folder_path: str, output_file: str) -> None:
    """
    Merge all CSV files in the given folder into a single CSV file.
    Each source file's name is added as a 'dataset' column.

    Args:
        folder_path (str): Path to the folder containing CSV files
        output_file (str): Path where the merged CSV file will be saved
    """
    # Get list of CSV files
    file_list = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    # Initialize an empty list to store DataFrames
    dfs = []
    print(f"Found {len(file_list)} CSV files to merge")
    # Read each CSV file and add to the list
    for file in file_list:
        file_path = os.path.join(folder_path, file)
        dataset_name = file.split('.')[0]
        print(f"Reading {file}...")
        # Read CSV in chunks to handle large files
        chunks = []
        for chunk in pd.read_csv(file_path, chunksize=10000, low_memory=False):
            try:
                chunk.rename(columns={'Unnamed: 0': 'cik_code'}, inplace=True)
            except Exception as e:
                pass
            chunk['dataset'] = dataset_name
            chunks.append(chunk)

        # Combine chunks for this file
        if chunks:
            file_df = pd.concat(chunks, ignore_index=True)
            dfs.append(file_df)

    # Combine all DataFrames
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        # remove rows which only have the first column and the rest empty
        merged_df.dropna(axis=0, how='all', subset=merged_df.columns[1:-3], inplace=True)
        print(f"Total rows: {len(merged_df)}")
        # remove duplicates
        merged_df.drop_duplicates(inplace=True)
        # Save to CSV
        merged_df.to_csv(output_file, index=False)
        print(f"Successfully merged {len(file_list)} files into {output_file}")
    else:
        print("No CSV files found to merge")


def clean_and_merge_columns(file_path: str, output_file: str) -> None:
    # Read the CSV file
    df = pd.read_csv(file_path, low_memory=False)

    # Clean column names by removing new lines
    df.columns = [col.replace('\n', '') for col in df.columns]

    # Merge columns with the same name
    df = df.groupby(axis=1, level=0).first()

    # Save the cleaned DataFrame to a new CSV file
    df.to_csv(output_file, index=False)


if __name__ == '__main__':
    # Example usage
    folder_path = f'{os.getenv("BASE_PATH")}/data/sentiment_results'
    output_file = f'{os.getenv("BASE_PATH")}/data/merged_sentiment_results_2.csv'
    merge_csv_files(folder_path, output_file)
    clean_and_merge_columns(output_file, output_file)
