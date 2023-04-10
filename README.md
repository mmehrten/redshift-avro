# redshift-avro

This repo demonstrates two ways to handle real-time Avro serialized data in Redshift. Redshift natively supports Avro data using the COPY command, but lacks Avro support for real-time data ingested in VARBINARY format.

## lambda-udf

This option uses a Lambda function to deserialize the Avro data and re-serialize it in JSON format. This method has cost and performance tradeoffs due to the Lambda function invocations, but will give flexibility and control in the long term, and is the simplest to implement and manage. The Lambda UDF in this example assumes the Avro data in Kinesis is a **list of records**. Depending on your schema, you may need to update your UDF accordingly.

1. Create a Lambda function and [Redshift Lambda UDF](https://docs.aws.amazon.com/redshift/latest/dg/udf-creating-a-lambda-sql-udf.html) to handle transforming hexadecimal encoded binary Avro data into JSON data
    * An example of this lambda function code is included using the [avro python library](https://pypi.org/project/avro/)
    * Note that you must create the UDF as IMMUTABLE type in Redshift so that Redshift knows your function is idempotent and doesn’t need to be re-run after it has run once for a given row. This will decrease your overall number of invocations
2. Perform the streaming data ingest as outlined in [this blog post](https://aws.amazon.com/blogs/big-data/real-time-analytics-with-amazon-redshift-streaming-ingestion/), loading the Kinesis or Kafka Avro data into Redshift in VARBINARY format
3. Select the binary data out of the materialized view
4. Encode it as a hexadecimal string to allow it to be sent to your Lambda function (since Redshift uses JSON to exchange data with Lambda)
5. Use the Lambda UDF to re-serialize the hex Avro data into JSON format
6. Use the builtin Redshift method json_parse to decode the JSON data as SUPER type
7. Perform other transformations on the SUPER data as needed

![Avro using a Lambda function](https://github.com/mmehrten/redshift-avro/Avro-Lambda.png)

## python-udf

Redshift also supports native Python UDFs. However, Redshift currently only supports Python 2.7, and requires you to provide your own code for decoding Avro data. This option makes it difficult to implement, as you will need to write your own Avro library. This project includes a Python 2.7 port of the [avro python library](https://pypi.org/project/avro/), which has been minimally tested.

1. Create a ZIP archive of the Python 2.7 Avro library port in S3, and create a [Redshift Python UDF](https://docs.aws.amazon.com/redshift/latest/dg/udf-python-language-support.html) to handle transforming hexadecimal encoded binary Avro data into JSON data
    * An example of this UDF function code is included using the [avro python library](https://pypi.org/project/avro/) Python 2.7 port
    * Note that the UDF needs a line added at the end `return decode(text)` when the UDF is registered with Redshift
    * Note that when defining the UDF input arguments (`(text varchar)`) Redshift will assume a 256 byte VARCHAR. You may need to explicitly set the size of the VARCHAR to be larger (e.g. `(text varchar(2500))`) to handle the size of your input payload - this only seems to be true of the input argument, not the return argument
2. Perform the streaming data ingest as outlined in [this blog post](https://aws.amazon.com/blogs/big-data/real-time-analytics-with-amazon-redshift-streaming-ingestion/), loading the Kinesis or Kafka Avro data into Redshift in VARBINARY format
3. Select the binary data out of the materialized view
4. Encode it as a hexadecimal string to allow it to be sent to your Python UDF function (since Redshift doesn't support VARBINARY data with UDFs)
5. Use the Python UDF to re-serialize the hex Avro data into JSON format
6. Use the builtin Redshift method json_parse to decode the JSON data as SUPER type
7. Perform other transformations on the SUPER data as needed

![Avro using a Python UDF](https://github.com/mmehrten/redshift-avro/Avro-Python.png)

## streaming

This option allows you to re-serialize the Avro data as JSON in-flight, and/or perform other ETL operations like normalizing the data, before it lands in Redshift. This solution would allow for a higher throughput without the Lambda function as a bottleneck, as Glue, EMR, and KDA have significantly increased scalability compared to Lambda. These solutions are more complex to manage and operate, but are more scalable for high throughput workloads.

Code example TBD.

![Streaming Ingest](https://github.com/mmehrten/redshift-avro/Streaming-Ingest.png)
