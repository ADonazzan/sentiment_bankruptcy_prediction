import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('../data/merged_bankrupt_companies.csv')

# Drop rows with missing CIK codes
df.dropna(subset=['CIK_extracted'], inplace=True)

# Melt the DataFrame to long format
# long_df = pd.melt(df, id_vars=['CIK_extracted', 'Ragione sociale', 'Codice NACE Rev. 2, core code (4 cifre)',
#                                'Codice di consolidamento', 'Ultimo anno disp.'], var_name='variable',
#                   value_name='value')
# df for bankrupt companies
long_df = pd.melt(df, id_vars=['NameCorp', 'YearFiled', 'CIK_extracted', 'Ragione sociale',
                               'Codice NACE Rev. 2, core code (4 cifre)',
                               'Codice di consolidamento', 'Ultimo anno disp.'], var_name='variable',
                  value_name='value')

# Extract the year and type of data from the variable column
long_df['year'] = long_df['variable'].str.extract(r'(\d{4})')

long_df['data_type'] = long_df['variable'].str.replace(r'\d{4}', '', regex=True).str.strip()

# Drop the original variable column
long_df.drop(columns=['variable'], inplace=True)

# Pivot the DataFrame to have one column per variable type
wide_df = long_df.pivot_table(
    index=['NameCorp', 'YearFiled', 'CIK_extracted', 'Ragione sociale', 'Codice NACE Rev. 2, core code (4 cifre)',
           'Codice di consolidamento', 'Ultimo anno disp.', 'year'], columns='data_type', values='value',
    aggfunc='first').reset_index()

# Save the transformed DataFrame to a new CSV file
wide_df.to_csv('../data/transformed_companies_bankrupt.csv', index=False)
