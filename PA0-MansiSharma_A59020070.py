#Acknowledgements - I would like to thank TAs - Megha and Rohit as well as my classmates - Gabriel and Yashashwita for their valuable feedbacks
%%time
import json
import ctypes
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)

def PA0(path):
    client = Client()
    # Helps fix any memory leaks. We noticed Dask can slow down when same code is run again.
    client.run(trim_memory)
    client = client.restart()

    # READ CSV FILE INTO DASK DATAFRAME
    df = dd.read_csv(path)
    # CODE STARTS
    df['helpful_votes'] = df['helpful'].str.strip('[]').str.split(',', n=1).str.get(0).astype(int)

    df['total_votes'] = df['helpful'].str.strip('[]').str.split(',', n=1).str.get(1).astype(int)

    df['reviewing_since'] = df['reviewTime'].str[-4:].astype(float)

    agg_dict = {
        'asin': 'count',
        'overall': 'mean',
        'reviewing_since': 'min',
        'helpful_votes': 'sum',
        'total_votes': 'sum'
    }


    meta = pd.DataFrame(columns=['asin', 'overall', 'reviewing_since', 'helpful_votes', 'total_votes'])

    grouped_df = df.groupby('reviewerID').agg(agg_dict, split_out=32, meta=meta)
    grouped_df = grouped_df.reset_index()
    grouped_df = grouped_df.rename(columns={'asin': 'number_products_rated', 'overall': 'avg_ratings'})
    
    grouped_df = grouped_df[[
        'reviewerID',
        'number_products_rated',
        'avg_ratings',
        'reviewing_since',
        'helpful_votes',
        'total_votes'
    ]]

    submit = grouped_df.describe().compute().round(2)    

    with open('results_PA0.json', 'w') as outfile:
        json.dump(json.loads(submit.to_json()), outfile)


