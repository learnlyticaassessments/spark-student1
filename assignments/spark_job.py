from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads the CSV file from the given file path and returns a DataFrame.
    """
    # TODO: implement using spark.read.csv(...)
    return spark.read.csv(file_path, header=True, inferSchema=True)

def process_data(df: DataFrame) -> DataFrame:
    """
    Processes the DataFrame (e.g., filter, group, aggregate) and returns the transformed DataFrame.
    """
    # TODO: implement your transformation logic here
    # Example: filter rows where value in column "age" is greater than 30
    return df.filter(df.age > 30)

def write_data(df: DataFrame, output_path: str) -> None:
    """
    Writes the DataFrame to the specified output path.
    """
    # TODO: implement writing, e.g., as Parquet
    df.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkAssignment").getOrCreate()
    input_file = "data.csv"
    output_dir = "output"
    
    df = read_data(spark, input_file)
    transformed_df = process_data(df)
    write_data(transformed_df, output_dir)
    
    spark.stop()
