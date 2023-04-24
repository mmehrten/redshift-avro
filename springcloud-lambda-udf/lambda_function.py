import io
import json
import struct
from typing import Dict, List

import aggregated_record_pb2
import avro.schema
import requests
from avro.datafile import DataFileReader
from avro.io import DatumReader

# TODO: Determine the real registry URL
REGISTRY_URL = "http://.../"


class SpringSchemaRegistry:
    """A Python implementation to interact with the Spring Cloud Schema Registry.

    Currently unimplemented. Need to understand the REST API structure for the registry,
    and how to use the registry responses with the Avro schema library."""

    def __init__(self, url: str):
        self._url = url
        self._schemas: Dict[str, avro.schema.Schema] = {}

    def _get_registry_response(self, schema_id: str) -> str:
        """Make a request for a schema to the schema registry.

        TODO: Implement

        :param schema_id: The schema to request
        :returns: The registry response text
        :raises: An HTTPError if a 4xx or 5xx status code is returned
        """
        resp = requests.get(self._url, json={"schema_id": schema_id})
        resp.raise_for_status()
        return resp.text

    def _parse_registry_response(self, response: str) -> avro.schema.Schema:
        """Parse the schema registry response into an Avro schema.

        TODO: Implement

        :param response: The registry response
        :returns: The Avro schema object
        """
        return avro.schema.parse(response)

    def get(self, schema_id: str) -> avro.schema.Schema:
        """Get an Avro schema from the registry by ID, caching and reusing the response.

        :param schema_id: The schema to request
        :returns: The Avro schema object
        """
        if schema_id not in self._schemas:
            data = self._get_registry_response(schema_id)
            self._schemas[schema_id] = self._parse_registry_response(data)
        return self._schemas[schema_id]


class SpringEmbeddedMessageUtils:
    """A Python port of the EmbeddedHeaderUtils from spring-cloud-stream.

    * [Docs](https://www.javadoc.io/doc/org.springframework.cloud/spring-cloud-stream/3.1.6/org/springframework/cloud/stream/binder/EmbeddedHeaderUtils.html#extractHeaders-org.springframework.messaging.Message-boolean-)
    * [Source](https://github.com/spring-cloud/spring-cloud-stream/blob/2cd7d07ba8a2cee723700b0da38ce3b9b1a167fa/core/spring-cloud-stream/src/main/java/org/springframework/cloud/stream/binder/EmbeddedHeaderUtils.java)
    """

    INTEGER_STRUCT_FORMAT = ">I"
    MAX_ONE_BYTE_INT: int = 0xFF
    MAX_FOUR_BYTE_INT: int = 0xFFFFFFFF

    @staticmethod
    def _read_uint(
        payload: io.BytesIO, bytesize: int = 4, max_size: int = MAX_FOUR_BYTE_INT
    ) -> int:
        """Read an unsigned int of a given byte size.

        :param payload: The BytesIO to read the integer from
        :param bytesize: The number of bytes to read from the stream
        :param max_size: The maximum integer size to read
        :returns: The integer from the stream
        """
        assert bytesize > 0 and bytesize <= 4
        padding = b"\x00" * (4 - bytesize)
        data = payload.read(bytesize)
        key = struct.unpack(
            SpringEmbeddedMessageUtils.INTEGER_STRUCT_FORMAT, padding + data
        )[0]
        return key & max_size

    @staticmethod
    def _read_string(payload: io.BytesIO, bytesize: int) -> str:
        """Read a string of a given byte size.

        :param payload: The BytesIO to read the integer from
        :param bytesize: The number of bytes to read from the stream
        :returns: The string from the stream
        """
        string = f"{bytesize}s"
        value = payload.read(bytesize)
        return struct.unpack(string, value)[0].decode("utf-8")

    @classmethod
    def get_message_headers(cls, payload: io.BytesIO) -> Dict[str, str]:
        """Read the embedded headers from a payload.

        :param payload: The BytesIO to read the headers from
        :returns: A dictionary with the headers, if the magic integer is found in the start of the stream.
            If the magic integer is not found at the start of the stream, an empty dict is returned.
        """
        magic_flag = cls._read_uint(payload, bytesize=1, max_size=cls.MAX_ONE_BYTE_INT)
        headers: Dict[str, str] = {}
        if magic_flag != cls.MAX_ONE_BYTE_INT:
            return headers
        header_count = cls._read_uint(
            payload, bytesize=1, max_size=cls.MAX_ONE_BYTE_INT
        )
        for _ in range(0, header_count):
            key_size = cls._read_uint(
                payload, bytesize=1, max_size=cls.MAX_ONE_BYTE_INT
            )
            key = cls._read_string(payload, key_size)
            value_size = cls._read_uint(
                payload, bytesize=4, max_size=cls.MAX_FOUR_BYTE_INT
            )
            value = cls._read_string(payload, value_size)
            headers[key] = json.loads(value)
        return headers


class KPLClient:
    """A client to decode KPL aggregated messages from a Kinesis stream.

    NOTE: Will not be necessary once Redshift implements this behavior in the coming weeks/months.
    When Redshift implements this behavior, the Redshift cluster will receive deaggregated messages, meaning
    the UDF will just need to decode the registry Avro.
    """

    @staticmethod
    def decode(payload: io.BytesIO) -> List[io.BytesIO]:
        """Decode a KPL aggregated response into a list of sub-payloads (the actual data)."""
        avro_records = aggregated_record_pb2.AggregatedRecords()
        avro_records = avro_records.ParseFromString(payload.read())
        return [io.BytesIO(record.data) for record in avro_records.records]


class LambdaHandler:
    """A class demonstrating various ways to handle Avro encoded data."""

    def __init__(self):
        self._registry = SpringSchemaRegistry(REGISTRY_URL)
        demo_schema = {
            "namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]},
            ],
        }
        self._demo_schema_avro = avro.schema.parse(json.dumps(demo_schema))

    @staticmethod
    def decode_avro(data: io.BytesIO, schema: avro.schema.Schema) -> Dict:
        """Decode a single Avro datum with a schema."""
        decoder = avro.io.BinaryDecoder(data)
        reader = avro.io.DatumReader(schema)
        return reader.read(decoder)

    @staticmethod
    def decode_file(data: io.BytesIO) -> List[Dict]:
        """Decode an Avro file encoded datum which includes the schema in the file."""
        decoder = DataFileReader(data, DatumReader())
        return list(decoder)

    def decode_spring_kpl_encoded_data(self, payload: io.BytesIO) -> List[Dict]:
        """Decode Spring-Cloud encoded data, which has used KPL consumer aggregation."""
        records = []
        for sub_payload in KPLClient.decode(payload):
            headers = SpringEmbeddedMessageUtils.get_message_headers(sub_payload)
            body = io.BytesIO(sub_payload.read())
            schema_id = headers["schema_id"]
            schema = self._registry.get(schema_id)
            record = self.decode_avro(body, schema)
            records.append(record)
        return records

    @staticmethod
    def _decode_redshift_payload(data: str) -> io.BytesIO:
        """Convert hex-encoded binary data into a BytesIO object."""
        return io.BytesIO(bytes.fromhex(data))

    @staticmethod
    def _redshift_success(data: List, num_records: int) -> str:
        """Return a Redshift success message."""
        return json.dumps(
            {
                "success": True,
                "num_records": num_records,
                "results": data,
            }
        )

    @staticmethod
    def _redshift_failure(error: Exception) -> str:
        """Return a Redshift failure message."""
        return json.dumps(
            {
                "success": False,
                "error_msg": f"Error processing Lambda event. Error: {error}",
            }
        )

    def file_handler(self, event, context):
        try:
            results = []
            for record in event["arguments"]:
                data_bytes = self._decode_redshift_payload(record[0])
                results.append(json.dumps(self.decode_file(data_bytes)))
            return self._redshift_success(results, event["num_records"])
        except Exception as e:
            return self._redshift_failure(e)

    def demo_handler(self, event, context):
        try:
            results = []
            for record in event["arguments"]:
                data_bytes = self._decode_redshift_payload(record[0])
                results.append(
                    json.dumps(self.decode_avro(data_bytes, self._demo_schema_avro))
                )
            return self._redshift_success(results, event["num_records"])
        except Exception as e:
            return self._redshift_failure(e)

    def springboot_handler(self, event, context):
        try:
            results = []
            for record in event["arguments"]:
                data_bytes = self._decode_redshift_payload(record[0])
                results.append(
                    json.dumps(self.decode_spring_kpl_encoded_data(data_bytes))
                )
            return self._redshift_success(results, event["num_records"])
        except Exception as e:
            return self._redshift_failure(e)


handler = LambdaHandler()
lambda_handler = handler.springboot_handler


def test_basic_message_decode():
    with open("kinesis_message_sample.avro", "rb") as f:
        byte_data = f.read()
    with open("schema.json", "r") as f:
        schema = avro.schema.parse(f.read())

    try:
        byte_io = io.BytesIO(byte_data)
        decoder = avro.io.BinaryDecoder(byte_io)
        reader = avro.io.DatumReader(schema)
        reader.read(decoder)
        print("Need schema in Lambda")
    except Exception as e:
        print(f"Failed to read with Schema: {e}")

    try:
        byte_io = io.BytesIO(byte_data)
        decoder = DataFileReader(byte_io, DatumReader())
        list(decoder)
        print("Schema is with payload")
    except Exception as e:
        print(f"Failed to read as file: {e}")


def test_get_message_headers():
    # From https://github.com/spring-cloud/spring-cloud-stream/blob/2cd7d07ba8a2cee723700b0da38ce3b9b1a167fa/core/spring-cloud-stream/src/test/java/org/springframework/cloud/stream/binder/MessageConverterTests.java#L45
    payload = io.BytesIO(
        b'\xff\x02\x03foo\x00\x00\x00\x05"bar"\x03baz\x00\x00\x00\x06"quxx"Hello'
    )
    assert SpringEmbeddedMessageUtils.get_message_headers(payload) == {
        "foo": "bar",
        "baz": "quxx",
    }
    assert payload.read().decode("utf-8") == "Hello"
