import pandas as pd

# Paths to the files
input_csv = "your_file.csv"  # CSV to be translated
translation_csv = "translation.csv"  # Translation dictionary CSV
output_csv = "translated_file.csv"  # Output CSV

# Read the translation CSV
translation_df = pd.read_csv(translation_csv, header=None)

# Extract the translation dictionary
translation_dict = dict(zip(translation_df.iloc[0], translation_df.iloc[1]))

# Read the input CSV
df = pd.read_csv(input_csv)

# Rename columns using the dictionary
df.rename(columns=translation_dict, inplace=True)

# Save the updated CSV
df.to_csv(output_csv, index=False)

print(f"Translated CSV saved to {output_csv}")
