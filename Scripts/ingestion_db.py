# import pandas as pd
# import os
# import sqlalchemy
# from sqlalchemy import create_engine
# import logging
# import time
# engine=create_engine('sqlite:///inventory.db')
# logging.basicConfig(
#     filename="logs/ingestion_db.log",
#     level=logging.DEBUG,
#     format="%(asctime)s-%(levelname)s-%(message)s",
#     filemode="a"
# )
# def ingest_db(df,table_name,engine):
#     '''ingest dataframe into database table'''
#     df.to_sql(table_name,con=engine,if_exists=replace,index=False)
# def load_raw_data():
#     ''' this function will load the CSVs as dataframe and ingest into db'''
#     start=time.time()
#     for file in os.listdir('data'):
#         df=pd.read_csv('data/'+file)
#         logging.info(f'Ingesting {file} in db')
#         ingest_db(df,file[:-4],engine)
#     end=time.time()
#     total_time=(end-start)/60
#     logging.info('-----------Ingestion Complete-----------')
#     logging.info(f'\nTotal Time Taken: {total_time} minutes')

# if __name__=='__main__':
#     load_raw_data()


import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine
import logging
import time

engine = create_engine('sqlite:///inventory.db')

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)

def ingest_db_in_chunks(file_path, table_name, engine, chunksize=100000):
    '''Ingest large CSV in chunks'''
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize)):
        if_exists = 'replace' if i == 0 else 'append'
        chunk.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        logging.info(f'Inserted chunk {i+1} of {file_path}')

def ingest_db(df, table_name, engine):
    '''Ingest small DataFrame into database'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    '''Load all CSV files in data/ into SQLite database'''
    start = time.time()
    for file in os.listdir('data'):
        file_path = os.path.join('data', file)
        table_name = file[:-4]

        try:
            # If it's a very large file like sales.csv, load in chunks
            if file.lower() == 'sales.csv':
                logging.info(f'Ingesting {file} in db using chunking...')
                ingest_db_in_chunks(file_path, table_name, engine)
            else:
                logging.info(f'Ingesting {file} in db...')
                df = pd.read_csv(file_path)
                ingest_db(df, table_name, engine)

        except Exception as e:
            logging.error(f"Failed to ingest {file}: {e}")

    end = time.time()
    total_time = (end - start) / 60
    logging.info('-----------Ingestion Complete-----------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()
