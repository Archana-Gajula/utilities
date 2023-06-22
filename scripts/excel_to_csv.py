import argparse

import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate csv file from Excel file.")
    parser.add_argument('--input', help='Input Excel file')
    parser.add_argument('--output', help='Output CSV file')
    args = parser.parse_args()

    df = pd.read_excel(args.input, dtype=str, engine='openpyxl')
    df.to_csv(args.output, index=False, sep='|', header='true')
