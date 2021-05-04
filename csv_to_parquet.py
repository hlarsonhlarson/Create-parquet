import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
import os


def my_reader_csv(inputFileName):
    file_df = pd.read_csv(inputFileName, header=None, delimiter=' ', names=['timestamp', 'price', 'volume'])
    convert = lambda x: datetime.datetime.fromtimestamp(x / 1e6)
    file_df['timestamp'] = file_df['timestamp'].apply(convert)
    return file_df
 
def csv_to_parquet(directory, outputFileName):
    fields = {"timestamp": pa.timestamp('ns'),
                "price": pa.float32(),
                 "volume": pa.float32()}
    schema = pa.schema(fields)
    with pq.ParquetWriter(outputFileName, schema=schema) as writer:
        for file in os.listdir(directory):
            inputFileName = directory + os.sep + str(file)
            print(inputFileName)
            file_df = my_reader_csv(inputFileName)
            insert_table = pa.Table.from_pandas(file_df, schema=schema)
            writer.write_table(insert_table)

if __name__ == '__main__':
    print('Enter path to directory')
    dir = input()
    print('Enter output file name')
    output = input()
    csv_to_parquet(dir, output)
