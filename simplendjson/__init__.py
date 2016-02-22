import pandas as pd
import json


def ndjson_to_dataframe(input_file_name):
    """ returns a dataframe from an ndjson file
    """
    with open(input_file_name, 'rb') as f:
        data = f.readlines()
    data = map(lambda x: x.rstrip(), data)
    data_json_str = "[" + ','.join(data) + "]"
    data_df = pd.read_json(data_json_str)
    return data_df


def dataframe_to_json(df):
    """ converts a dataframe to a json array
    """
    return json.loads(df.to_json(orient="records"))


def read_ndjson(input_file_name):
    """ returns a json array from an ndjson file
    """
    with open(input_file_name, 'rb') as f:
        return [json.loads(l) for l in f.readlines()]


def read_ndjson_streaming(input_file_name):
    with open(input_file_name, 'rb') as f:
        for line in f:
            yield(json.loads(line))


def write_ndjson(json_arr, output_file_name):
    """ writes a json_array to an ndjson file
    """
    with open(output_file_name, 'wb') as f:
        for l in json_arr:
            f.write(json.dumps(l))
            f.write('\n')
