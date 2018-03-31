# SparkETL

### Extract
#### SparkETL is a library for performing ETL on Apache Spark using python.
#### It provides with tools to Read Data from various sources, Apply Transformations on the data read and then load them to the destination.
#### It Currently can read -
* Delimited Files
* JSONS
* Fixed length record files
* ZIP files which contains delimited text files
* Avro | dependency - databricks-avro jar

### Transformation
#### CDC to capture changes in dimension. Performs Upsert, doesn't support delete of records.
#### Transformer - applies tranformation on columns
