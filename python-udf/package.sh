#! /bin/sh

if [ -e avro.zip ]; then rm avro.zip; fi
zip -r avro.zip avro
aws s3 cp avro.zip "s3://${BUCKET}"
