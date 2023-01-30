import requests
import pandas as pd
from elasticsearch import Elasticsearch
from movie_mapping.mapping import movies_mappings

"""to check elasticsearch is up or not"""
substring = "You Know, for Search".encode()
response = requests.get("http://127.0.0.1:9200")
if substring in response.content:
   print("Elasticsearch is up and running!")
else:
   print("Something went wrong, ensure the cluster is up!")


"""function to store data on elasticsearch """
def elasticstore(inputfilepath, indexname):

   """to read movies.csv"""
   df =(
      pd.read_csv(inputfilepath).iloc[:,:-1] 
      .dropna()
      .sample(200, random_state=42)
      .reset_index()   
   )

   """connect elasticsearch"""
   es = Elasticsearch("http://127.0.0.1:9200")

   es.info()

   """create new index"""
   es.indices.create(indexname)

   """create mapping """
   es.indices.put_mapping(index=indexname, body=movies_mappings)  


   for i,row in df.iterrows():

      doc = {
         "name":row["name"],
         "rating":row["rating"],
         "genre":row["genre"],
         "year":row["year"],
         "score":row["score"],
         "votes":row["votes"],
         "director":row["director"],
         "writer":row["writer"],
         "star":row["star"],
         "country":row["country"],
         "budget":row["budget"],
         "gross":row["gross"],
         "company":row["company"],
         "runtime":row["runtime"]
      }

      es.index(index=indexname, id=i, body= doc)
      es.indices.refresh(index=indexname)

   print("process done")

   return True

"""take file path and index name by user"""

inputfilename = input("enter the path of  csv file:    ")

indexname = input("enter index name here:   ")

elasticstore(inputfilename, indexname)