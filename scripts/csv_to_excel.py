import argparse

import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate Excl file from CSV file.")
    parser.add_argument('--input', help='Input CSV file')
    parser.add_argument('--output', help='Output Excel file')
    args = parser.parse_args()

    df = pd.read_csv(args.input, delimiter='|', header=0,
                     low_memory=False)
    df.to_excel(args.output, columns=df.columns, index=False)


