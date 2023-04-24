import io
import json

from avro.datafile import DataFileReader
from avro.io import DatumReader


def lambda_handler(event, context):
    try:
        return {
            "success": True,
            "num_records": event["num_records"],
            "results": [
                list(
                    json.dumps(
                        list(
                            DataFileReader(
                                io.BytesIO(bytes.fromhex(record[0])), DatumReader()
                            )
                        )
                    )
                )
                for record in event["arguments"]
            ],
        }
    except Exception as e:
        return {
            "success": False,
            "error_msg": f"Error processing Lambda event. Error: {e}",
        }
