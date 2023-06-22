import argparse

import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate Excl file from CSV file.")
    parser.add_argument('--input', help='Input parquet file')
    parser.add_argument('--output', help='Output Excel file')
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    df.to_excel(args.output, columns=df.columns, index=False)
