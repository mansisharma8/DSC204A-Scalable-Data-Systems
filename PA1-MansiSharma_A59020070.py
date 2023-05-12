
#final code

import time
import json
import dask.dataframe as dd
from dask.distributed import Client
import ctypes
import ast
import dask
def trim_memory() -> int:
    """
    helps to fix any memory leaks.
    """
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)
         
def PA1(reviews_csv_path,products_csv_path):
  start = time.time()
  client = Client('127.0.0.1:8786')
  #client = Client(n_workers = 4)
  client.run(trim_memory)
  client = client.restart()
  print(client)
  reviews_df = dd.read_csv(reviews_csv_path)
  products_df = dd.read_csv(products_csv_path, dtype={'asin': 'object','categories': 'object','related':'object'})
  # WRITE YOUR CODE HERE   
  #Q1
  
  reviews_missing_percent = (reviews_df.isnull().mean() * 100).compute().round(2)
  reviews_missing_percent = reviews_missing_percent.to_dict()
  ans1 = reviews_missing_percent
  
  #Q2
  products_missing_percent = (products_df.isnull().mean() * 100).compute().round(2)
  products_missing_percent = products_missing_percent.to_dict()
  ans2 = products_missing_percent
  
  #Q3
  pa = products_df[['asin','price']]
  ra = reviews_df[['asin','overall']]
  joined_df = dd.merge(pa, ra, on='asin')
  #Select the 'price' and 'overall' columns from the joined table
  price_rating_df = joined_df[['price', 'overall']]
  
  # Compute the Pearson correlation coefficient and extract the value
  pearson_corr = price_rating_df.corr().compute().values[0,1]
  ans3 = pearson_corr
  ans3 = round(float(ans3),2)
  
  #Q4
  price_stats = dd.compute(products_df['price'].max(), products_df['price'].mean(),
                         products_df['price'], products_df['price'].min(), 
                         products_df['price'].std())

  ans4 = {'max': round(price_stats[0], 2),
        'mean': round(price_stats[1], 2),
        'median': round(price_stats[2].median(), 2),
        'min': round(price_stats[3], 2),
        'std': round(price_stats[4], 2)}
  
  
  #Q5
  
  products_df = products_df.dropna(subset=['categories'])

# create a new column 'super_category' by extracting the first category
  products_df['super_category'] = products_df['categories'].apply(lambda x: eval(x)[0][0], meta=('categories', 'object'))


  # get the count of each super category and convert it to a dictionary
  super_cat_counts = products_df['super_category'].value_counts().compute().sort_values(ascending=False)

  super_cat_counts = super_cat_counts.to_dict()

  super_cat_counts = {k: v for k, v in super_cat_counts.items() if k is not None}
  ans5 = super_cat_counts
  del ans5['']
  
  
   #Q6
   
  ans6 = 0
  asin_set = {val: False for val in products_df['asin'].compute()}
  for z in reviews_df['asin']:
    if ans6 == 1:
        break
    if z not in asin_set:
        ans6 = 1
        break
  
   #Q7
  products_df['related'] = products_df['related'].fillna('{}')
  def convert_dict(dict_str):
      return ast.literal_eval(dict_str)
  products_df['related'] = products_df['related'].apply(convert_dict, meta=('rel', 'object'))

  related_set = set()
  ans7 = 0
  for related_dict in products_df['related']:
    if ans7 == 1:
        break
    for asin in related_dict.values():
        if ans7 == 1:
            break
        for p in asin:
            if p not in asin_set:
                ans7 = 1
                #print(ans7)
                break
  

  
  assert type(ans1) == dict, f"answer to question 1 must be a dictionary like {{'reviewerID':0.2, ..}}, got type = {type(ans1)}"
  assert type(ans2) == dict, f"answer to question 2 must be a dictionary like {{'asin':0.2, ..}}, got type = {type(ans2)}"
  assert type(ans3) == float, f"answer to question 3 must be a float like 0.8, got type = {type(ans3)}"
  assert type(ans4) == dict, f"answer to question 4 must be a dictionary like {{'mean':0.4,'max':0.6,'median':0.6...}}, got type = {type(ans4)}"
  assert type(ans5) == dict, f"answer to question 5 must be a dictionary, got type = {type(ans5)}"         
  assert ans6 == 0 or ans6==1, f"answer to question 6 must be 0 or 1, got value = {ans6}" 
  assert ans7 == 0 or ans7==1, f"answer to question 7 must be 0 or 1, got value = {ans7}" 
  
  end = time.time()
  runtime = end-start
  print(f"runtime  = {runtime}s")
  ans_dict = {
      "q1": ans1,
      "q2": ans2,
      "q3": ans3,
      "q4": ans4,
      "q5": ans5,
       "q6": ans6,
      "q7": ans7,
      "runtime": runtime
  }
  with open('results_PA1.json', 'w') as outfile: json.dump(ans_dict, outfile)       
  return runtime  
    







