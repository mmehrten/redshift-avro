import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io


def decode(text):
    df = DataFileReader(io.BytesIO(text.decode("hex")), DatumReader())
    return json.dumps(list(df))
