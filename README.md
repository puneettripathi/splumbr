# SparkETL
##### SparkETL is a library for performing ETL on Apache Spark using python.
##### It provides with tools to Read Data from various sources, Apply Transformations on the data read and then load them to the destination.

### io
#### It Currently can read -
* Delimited Files
* JSONS
* Fixed length record files
* ZIP files which contains delimited text files
* Avro | dependency - databricks-avro jar
* Parquet
* Hive Tables
#### It Currently can write to -
* Delimited Files
* JSONs
* Avro | dependency - databricks-avro jar
* ORC
* Parquet
* Hive Tables

##### It handles mismatch in schema of Hive tables while writing out of the box.

### Transformation
#### CDC to capture changes in dimension. 
* Performs Upsert
* doesn't support delete of records.
*  <i>-- More like SCD I</i>

#### Transformer - applies tranformation on columns
* apply udf to all string columns
* single columns transformations
* drop multiple columns
* keep columns
* outlier detection and handling
* missing value imputation with - mean, meadian, mode, constant

#### Quality Assuarance
* Report on all columns
* comparison of data in two dataframes

<b> It is a work in progress </b>
