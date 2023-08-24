import functools
import io
import json
import os

import avro.io
import avro.schema
import boto3

glue = boto3.client("glue")
REGISTRY_NAME = os.environ["GLUE_REGISTRY_NAME"]


@functools.lru_cache(maxsize=32)
def _get_schema(stream_name: str) -> avro.schema.Schema:
    """Get an Avro schema from the registry by stream name.

    :param stream_name: The stream name for the schema to request
    :returns: The Avro schema object
    """
    schema_id = {"RegistryName": REGISTRY_NAME, "SchemaName": stream_name}
    schema_resp = glue.get_schema_version(
        SchemaId=schema_id,
        SchemaVersionNumber={"LatestVersion": True},
    )
    schema = schema_resp["SchemaDefinition"]
    return avro.schema.parse(schema)


def _avro_to_json(stream_name: str, data: str) -> str:
    """Decode a single Hex-encoded Avro datum using the schema associated with the stream name.

    :param stream_name: The stream name for the data
    :param data: The Hex-encoded Avro binary data
    :returns: A JSON encoded version of the data
    """
    schema = _get_schema(stream_name)
    data_bytes = io.BytesIO(bytes.fromhex(data))
    decoder = avro.io.BinaryDecoder(data_bytes)
    reader = avro.io.DatumReader(schema)
    decoded = reader.read(decoder)
    return json.dumps(decoded)


def lambda_handler(event, context):
    try:
        results = []
        for stream_name, data in event["arguments"]:
            results.append(_avro_to_json(stream_name, data))
        return json.dumps(
            {
                "success": True,
                "num_records": event["num_records"],
                "results": results,
            }
        )
    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error_msg": f"Error processing Lambda event. Error: {e}. Event: {event}",
            }
        )


def test_lambda_handler():
    schema = avro.schema.parse(
        json.dumps(
            {
                "type": "record",
                "name": "User",
                "namespace": "example.avro",
                "fields": [
                    {"type": "string", "name": "name"},
                    {"type": ["int", "null"], "name": "favorite_number"},
                    {"type": ["string", "null"], "name": "favorite_color"},
                ],
            }
        )
    )
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(
        {"name": "Moiraine", "favorite_color": "Blue", "favorite_number": 4}, encoder
    )
    data = bytes_writer.getvalue().hex()
    result = json.loads(
        lambda_handler({"arguments": [["avro", data]], "num_records": 1}, None)
    )
    assert result["success"]
    assert (
        result["results"][0]
        == '{"name": "Moiraine", "favorite_number": 4, "favorite_color": "Blue"}'
    )
