import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, array, when, array_remove, lit, size, array_contains


def get_schema(df):
    schema_list = df.dtypes
    schema_list.sort()
    return schema_list


def get_schema_diff(df_1, df_2):
    list_1 = get_schema(df_1)
    list_2 = get_schema(df_2)
    diff = [column for column in list_1 if column not in list_2]
    return diff


def get_common_columns(df_1, df_2):
    df1_columns = df_1.columns
    df2_columns = df_2.columns
    all_col = set(df1_columns + df2_columns)
    columns = [column for column in all_col if column in df1_columns and column in df2_columns]
    return columns


def read_file(file_path):
    file_type = file_path.split('.')[-1]
    if file_type == 'json':
        df = spark.read.option("multiLine", "true").json(file_path)
    elif file_type == 'csv':
        df = spark.read.csv(file_path, sep=',', header=True)
    elif file_type == 'parquet':
        df = spark.read.parquet(file_path)
    else:
        raise ValueError(f'file type {file_type} is not supported')
    return df


if __name__ == '__main__':
    script_dir = os.path.abspath(os.path.dirname(__file__))
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--expected_file_path", help="expected file path from repository root")
    parser.add_argument("-a", "--actual_file_path", help="actual file path from repository root")
    parser.add_argument("-pk", "--PK", help="Primary Key column")
    args = parser.parse_args()

    expected_file_path = os.path.realpath(f'{script_dir}/../{args.expected_file_path}')
    actual_file_path = os.path.realpath(f'{script_dir}/../{args.actual_file_path}')
    primary_key_column = args.PK

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    actual_df = read_file(actual_file_path)
    expected_df = read_file(expected_file_path)

    # actual_df = actual_df.select('legacy_offer_data.*', '*')
    # expected_df = expected_df.select('legacy_offer_data.*', '*')

    print("columns not in actual_df")
    diff_col_actual = get_schema_diff(expected_df, actual_df)
    print(diff_col_actual)

    print("columns not in expected_df")
    diff_col_expected = get_schema_diff(actual_df, expected_df)
    print(diff_col_expected)

    print("Count of all records from expected:", expected_df.count())
    print("Distinct Count of records from expected:", expected_df.distinct().count())
    print("Count of all records from actual:", actual_df.count())
    print("Distinct Count of records from actual:", actual_df.distinct().count())

    common_columns = get_common_columns(actual_df, expected_df)
    actual_df = actual_df.select(common_columns)
    expected_df = expected_df.select(common_columns)

    common_records_df = expected_df.intersect(actual_df)
    print("Count of records that are in both:", common_records_df.count())

    only_expected_df = expected_df.subtract(actual_df)
    print("###### only_expected_df ###")
    only_expected_df.show()

    only_actual_df = actual_df.subtract(expected_df)
    print("###### only_actual_df ######")
    only_actual_df.show()

    print("Some non common records from expected:")
    expected_non_matching_records = only_expected_df.join(only_actual_df, on=primary_key_column, how='inner').select(
        only_expected_df["*"])
    expected_non_matching_records.orderBy(F.col(primary_key_column).asc()).show(5, False)

    print("Some non common records from actual:")
    actual_non_matching_records = only_actual_df.join(only_expected_df, on=primary_key_column, how='inner').select(
        only_actual_df["*"])
    actual_non_matching_records.orderBy(F.col(primary_key_column).asc()).show(5, False)

    # compare records column wise and adding non matching column names into array
    conditions = [when(expected_df[c] != actual_df[c], lit(c)).otherwise("") for c in expected_df.columns]
    #     print("conditions:",conditions)
    select_exp = [col(primary_key_column), array_remove(array(*conditions), "").alias("column_names")]
    #     print("select_exp:",select_exp)
    mismatch_columns_df = expected_df.join(actual_df,
                                           on=primary_key_column).select(select_exp)
    #     mismatch_columns_df.printSchema()

    print('Count of all records:', mismatch_columns_df.count())
    print('Count of records having mismatch:', mismatch_columns_df.filter(size("column_names") > 0).count())
    print('Count of records matching:', mismatch_columns_df.filter(size("column_names") == 0).count())
    # print the records having mismatch
    mismatch_columns_df.filter(size("column_names") > 0).show(truncate=False)

    # records having mismatch for perticular column
    # mismatch_columns_df.filter(array_contains(col('column_names'), "pvvversionno")).filter(
    #     size("column_names") > 0).show(truncate=False)

    mismatch_columns_df.select('column_names').distinct().show(truncate=False)


