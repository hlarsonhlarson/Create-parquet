import datetime
import os
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


def getTimestamp(fileName, seconds, milliseconds):
    date = datetime.datetime.strptime(fileName, "%Y%m%d")
    date += datetime.timedelta(seconds=seconds)
    date += datetime.timedelta(milliseconds=milliseconds)
    return date

def data_extractor(fileName, fullPath):
    with open(fullPath, 'rb') as file:
        file.read(10)
        seconds = int.from_bytes(file.read(4), byteorder='little')
        milliseconds = int.from_bytes(file.read(4), byteorder='little')
        price = int.from_bytes(file.read(4), byteorder='little')
        volume = int.from_bytes(file.read(4), byteorder='little')
    return getTimestamp(fileName, seconds, milliseconds), price, volume

def writer(path, output_file):
    directory = path
    fields = {"timestamp": pa.timestamp('ms'),
            "price": pa.int32(),
            "volume": pa.int32()}
    schema = pa.schema(fields)
    with pq.ParquetWriter(output_file, schema=schema) as writer:
        for file in os.listdir(directory):
            filename = directory + os.sep + str(file)
            print(filename)
            date, price, volume = data_extractor(str(file), filename)
            row = pd.DataFrame([[date,price,volume]],columns=['timestamp', 'price', 'volume'])
            insert_table = pa.Table.from_pandas(row, schema=schema)
            writer.write_table(insert_table)


if __name__ == '__main__':
    print('Enter path to directory with input files')
    path = input()
    print('Enter name of output file')
    output_file = input()
    writer(path, output_file)
