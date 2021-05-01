import pyarrow.parquet as pq


if __name__ == '__main__':
    print('Enter path to parquet file')
    name = input()
    table = pq.read_table(name)
    print(table.to_pandas())
