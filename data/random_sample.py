import csv
import pandas as pd
import random

def generate_sampled_data(df, limit, include_headers=True):

    file_name = f'random_{limit}.csv'

    # Ensure limit doesn't exceed the available rows
    limit = min(limit, df.shape[0])
    indices = random.sample(range(df.shape[0]), limit)

    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write headers if specified
        if include_headers:
            writer.writerow(df.columns)

        # Write sampled rows
        for idx in indices:
            writer.writerow(df.iloc[idx].to_list())

    print(f"File '{file_name}' successfully created with {limit} rows.")

# Example usage
# if __name__ == "__main__":
    # Generate a random malignant dataset with 30 samples
    # df = pd.read_excel('/content/data_bal - 20000.xlsx')
    # generate_sampled_data(df, limit=10000)
