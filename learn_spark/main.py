"""
A place to learn pyspark"""

import os
import pandas as pd
from pyspark.sql import SparkSession

CURR_DIR = os.path.abspath(os.path.dirname(__file__))
DOC_NAME = os.path.join(CURR_DIR, '..', "docs", "employee_dataset.csv")


def df_pandas():
    """
    Try to verify first the input file with pandas
    """
    df = pd.read_csv(DOC_NAME)
    df_describe = df.describe()
    print(df_describe)


def main():
    """
    Main function executing necessary commmands
    """
    df_pandas()


def init():
    """
    init function call main function
    """
    if __name__ == "__main__":
        main()

init()