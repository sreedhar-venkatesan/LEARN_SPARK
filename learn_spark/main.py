"""
A place to learn pyspark
"""

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

CURR_DIR = os.path.abspath(os.path.dirname(__file__))
DOC_NAME = os.path.join(CURR_DIR, '..', "docs", "employee_dataset.csv")


def pandas_df():
    """
    Try to verify first the input file with pandas
    """
    df = pd.read_csv(DOC_NAME)
    df_describe = df.describe()
    print(df_describe)

def spark_df():
    """
    All spark related commands
    """
    spark = SparkSession.builder.appName('Practise').getOrCreate()
    df_pyspark = spark.read.csv(DOC_NAME, header=True, inferSchema=True)
    df_pyspark.show()
    imputer = Imputer(
        inputCols = ['Age', 'Experience', 'Salary'],
        outputCols = ["{}_imputed".format(c) for c in ['age', 'Experience','Salary']]
    ).setStrategy("mean")
    imputer.fit(df_pyspark).transform(df_pyspark).show()

def main():
    """
    Main function executing necessary commmands
    """
    spark_df()


def init():
    """
    init function call main function
    """
    if __name__ == "__main__":
        main()

init()