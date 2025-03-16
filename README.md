Assignment Overview
Assignment:
Implement a Spark job (using PySpark) that performs basic data processing. 

For example, students should create a job that:

Reads a CSV file (e.g. data.csv provided in the repo root).

Performs a transformation (e.g. filters rows based on a condition or aggregates data).

Writes the result to an output directory (or returns a DataFrame).

Functions to Implement (Example):

read_data(spark, file_path):

Input: A SparkSession object and a file path to a CSV file.

Output: A DataFrame containing the data.

process_data(df):

Input: A Spark DataFrame.

Output: A transformed DataFrame (for example, filtered or aggregated).

write_data(df, output_path):

Input: A Spark DataFrame and an output file path.

Action: Writes the DataFrame to disk.
