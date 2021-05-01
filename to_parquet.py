import datetime
import os
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


SPACE_SIZE = 10
PRICE_SIZE = 4
VOLUME_SIZE = 4
SECONDS_SIZE = 4
MILLIESECONDS_SIZE = 4


def getTimestamp(fileName, seconds, milliseconds):
    date = datetime.datetime.strptime(fileName, "%Y%m%d")
    date += datetime.timedelta(seconds=seconds)
    date += datetime.timedelta(milliseconds=milliseconds)
    return date

def from_byte_to_values(b, fileName):
    start = 0
    seconds = int.from_bytes(b[start:start+SECONDS_SIZE], byteorder='little')
    start += SECONDS_SIZE
    milliseconds = int.from_bytes(b[start:start+MILLIESECONDS_SIZE], byteorder='little')
    start += MILLIESECONDS_SIZE
    price = int.from_bytes(b[start:start+PRICE_SIZE], byteorder='little')
    start += PRICE_SIZE
    volume = int.from_bytes(b[start:start+VOLUME_SIZE], byteorder='little')
    return [getTimestamp(fileName, seconds, milliseconds), price, volume]

def data_extractor(fileName, fullPath):
    file_info = []
    CHUNK_SIZE = SPACE_SIZE + SECONDS_SIZE + MILLIESECONDS_SIZE + PRICE_SIZE + VOLUME_SIZE
    with open(fullPath, 'rb') as file:
        while True:
            b = file.read(CHUNK_SIZE)
            if len(b) < CHUNK_SIZE:
                break
            file_info.append(from_byte_to_values(b, fileName))
    return file_info

def writer(path, output_file):
    directory = path
    fields = {"timestamp": pa.timestamp('ms'),
            "price": pa.int16(),
            "volume": pa.int16()}
    schema = pa.schema(fields)
    with pq.ParquetWriter(output_file, schema=schema) as writer:
        for file in os.listdir(directory):
            filename = directory + os.sep + str(file)
            print(filename)
            file_info = data_extractor(str(file), filename)
            for date, price, volume in file_info:
                row = pd.DataFrame([[date,price,volume]],columns=['timestamp', 'price', 'volume'])
                insert_table = pa.Table.from_pandas(row, schema=schema)
                writer.write_table(insert_table)


if __name__ == '__main__':
    print('Enter path to directory with input files')
    path = input()
    print('Enter name of output file')
    output_file = input()
    writer(path, output_file)
