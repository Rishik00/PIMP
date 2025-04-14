import argparse
import pandas as pd
import random
import csv

def generate_sampled_data(df, limit, output_file, include_headers=True, seed=None):
    # Set random seed for reproducibility
    if seed is not None:
        random.seed(seed)

    # Ensure limit doesn't exceed the available rows
    limit = min(limit, df.shape[0])
    indices = random.sample(range(df.shape[0]), limit)

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write headers if specified
        if include_headers:
            writer.writerow(df.columns)

        # Write sampled rows
        for idx in indices:
            writer.writerow(df.iloc[idx].to_list())

    print(f"File '{output_file}' successfully created with {limit} rows (Seed: {seed}).")

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Randomly sample rows from a CSV file and save to a new file.")
#     parser.add_argument("file_path", type=str, help="Path to the input CSV file.")
#     parser.add_argument("limit", type=int, help="Number of rows to sample.")
#     parser.add_argument("--output", type=str, default=None, help="Output CSV file name (default: random_<limit>.csv)")
#     parser.add_argument("--no_headers", action="store_true", help="Exclude headers from the output file.")
#     parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility (default: None)")

#     args = parser.parse_args()
#     df = pd.read_csv(args.file_path)

#     # Set output file name
#     output_file = args.output if args.output else f"random_{args.limit}.csv"
#     generate_sampled_data(df, args.limit, output_file, include_headers=not args.no_headers, seed=args.seed)
