import weaviate # to communicate to the Weaviate instance
import time
from weaviate import Client
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
#setting up client
load_dotenv()
username = os.getenv('WEAVIATE_USERNAME')
password = os.getenv('WEAVIATE_PASSWORD')
url = os.getenv('WEAVIATE_URL')
openai_api_key = os.getenv('OPENAI_API_KEY')

# Configure the Weaviate client
my_credentials = weaviate.auth.AuthClientPassword(
    username=username,
    password=password
)
client = weaviate.Client(
    url=url,
    auth_client_secret=my_credentials,
    additional_headers={
        "X-OpenAI-Api-Key": openai_api_key
    },
)

data = pd.read_csv("<---CSV FILE PATH--->")
def configure_batch(client: Client, batch_size: int, batch_target_rate: int):
    """
    Configure the weaviate client's batch so it creates objects at `batch_target_rate`.

    Parameters
    ----------
    client : Client
        The Weaviate client instance.
    batch_size : int
        The batch size.
    batch_target_rate : int
        The batch target rate as # of objects per second.
    """

    def callback(batch_results: dict) -> None:

        # you could print batch errors here
        time_took_to_create_batch = batch_size * (client.batch.creation_time/batch_size)
        time.sleep(
            1
            # max(batch_size/batch_target_rate - time_took_to_create_batch + 1, 0)
        )

    client.batch.configure(
        batch_size=batch_size,
        timeout_retries=5,
        callback=callback,
    )
configure_batch(client,60,50)
if len(data) == 0:
  print("NO DATA OBJECTS IN CSV FILE")
#importing the data objects
for i in range (0,len(data)):
    where_filter = {
    "path": ["index"],
    "operator": "Equal",
    "valueNumber": int(data.iloc[i]['index'])
    }
    query_result = (
    client.query
    .get("NewDocument","_additional{id}")
    .with_where(where_filter)
    .do()
    )
    # print(query_result)   
        
    if len(query_result['data']['Get']['NewDocument'])>0:
            doc_id=query_result['data']['Get']['NewDocument'][0]['_additional']['id']
            # print(doc_id)
            client.data_object.delete(doc_id, "NewDocument")
            try:
                en = int(data.iloc[i]["episodenumber"])
            except:
                en = -1
            try:
                pt = int(data.iloc[i]["part"])
            except:
                pt = -1
            try:
                bn = int(data.iloc[i]["booknumber"])
            except:
                bn = -1
            obj = {
                "index": int(data.iloc[i]["index"]),
                "holder": str(data.iloc[i]["holder"]),
                "production": str(data.iloc[i]["production"]),
                "episodenumber": en,
                "part": pt,
                "aititle": str(data.iloc[i]["aititle"]),
                "aisubtitle": str(data.iloc[i]["aisubtitle"]),
                "aikeywords": str(data.iloc[i]["aikeywords"]),
                "bibleverses": str(data.iloc[i]["bibleverses"]),
                "biblecharacters": str(data.iloc[i]["biblecharacters"]),
                "bibleconcepts": str(data.iloc[i]["bibleconcepts"]),
                "famouspeople": str(data.iloc[i]["famouspeople"]),
                "booksmentioned": str(data.iloc[i]["booksmentioned"]),
                "lifeissues": str(data.iloc[i]["lifeissues"]),
                "biblicallesson": str(data.iloc[i]["biblicallesson"]),
                "questionanswered": str(data.iloc[i]["questionanswered"]),
                "bookofthebible": str(data.iloc[i]["bookofthebible"]),
                "aifirstgrader": str(data.iloc[i]["aifirstgrader"]) if "aifirstgrader" in data.iloc[i] else '',
                "aisimple": str(data.iloc[i]["aisimple"]),
                "aielegant": str(data.iloc[i]["aielegant"]),
                "aicreative": str(data.iloc[i]["aicreative"]),
                "aibiblical": str(data.iloc[i]["aibiblical"]),
                "aicasual": str(data.iloc[i]["aicasual"]),
                "aiformal": str(data.iloc[i]["aiformal"]),
                "ainewsanchor": str(data.iloc[i]["ainewsanchor"]),
                "ailoving": str(data.iloc[i]["ailoving"]),
                "importantphrase": str(data.iloc[i]["importantphrase"]),
                "christiantopics": str(data.iloc[i]["christiantopics"]),
                "biblicalconcepts": str(data.iloc[i]["biblicalconcepts"]),
                "describingwords": str(data.iloc[i]["describingwords"]),
                "biblereferences": str(data.iloc[i]["biblereferences"]),
                "biblephrases": str(data.iloc[i]["biblephrases"]),
                "aiphdstudent": str(data.iloc[i]["aiphdstudent"]),
                "booknumber": bn,
                "episodetitle": str(data.iloc[i]["episodetitle"]),
                "filename": str(data.iloc[i]["filename"]),
                "paragraph": str(data.iloc[i]["paragraph"]),
                "summary": str(data.iloc[i]["summary"]),
                "productionimage": str(data.iloc[i]["productionimage"]),
                "publisher": str(data.iloc[i]["publisher"]),
                "publisherimage": str(data.iloc[i]["publisherimage"]),
                "testament": str(data.iloc[i]["testament"]),
                "type": str(data.iloc[i]["type"]),
                "booktitle": str(data.iloc[i]["booktitle"]),
                "booksubtitle": str(data.iloc[i]["booksubtitle"]),
                "closest": str(data.iloc[i]["closest"]),
                "month": str(data.iloc[i]["month"]),
                "year": str(data.iloc[i]["year"]),
                "isbn": str(data.iloc[i]["isbn"]),
                "author": str(data.iloc[i]["author"]),
                "pages": str(data.iloc[i]["pages"]),
                "journal": str(data.iloc[i]["journal"]),
                "ext1": str(data.iloc[i]["ext1"]),
                "ext2": str(data.iloc[i]["ext2"]),
                "ext3": str(data.iloc[i]["ext3"]),
                "ext4": str(data.iloc[i]["ext4"]),
                "ext5": str(data.iloc[i]["ext5"]),
                "public": "",
                "prodimage": "",
                "prodname": ""
            }
                #checking the items imported
            # client.data_object.create(obj, "Document")
            # print(str(i+1)+"/"+str(len(data))+" Completed")
            try:
                client.batch.add_data_object(obj, "NewDocument")
                print(str(i+1)+"/"+str(len(data))+" Completed")
            except:
                print("object at "+str(i)+" Failed")
    else:
            try:
                en = int(data.iloc[i]["episodenumber"])
            except:
                en = -1
            try:
                pt = int(data.iloc[i]["part"])
            except:
                pt = -1
            try:
                bn = int(data.iloc[i]["booknumber"])
            except:
                bn = -1
            obj = {
                "index": int(data.iloc[i]["index"]),
                "holder": str(data.iloc[i]["holder"]),
                "production": str(data.iloc[i]["production"]),
                "episodenumber": en,
                "part": pt,
                "aititle": str(data.iloc[i]["aititle"]),
                "aisubtitle": str(data.iloc[i]["aisubtitle"]),
                "aikeywords": str(data.iloc[i]["aikeywords"]),
                "bibleverses": str(data.iloc[i]["bibleverses"]),
                "biblecharacters": str(data.iloc[i]["biblecharacters"]),
                "bibleconcepts": str(data.iloc[i]["bibleconcepts"]),
                "famouspeople": str(data.iloc[i]["famouspeople"]),
                "booksmentioned": str(data.iloc[i]["booksmentioned"]),
                "lifeissues": str(data.iloc[i]["lifeissues"]),
                "biblicallesson": str(data.iloc[i]["biblicallesson"]),
                "questionanswered": str(data.iloc[i]["questionanswered"]),
                "bookofthebible": str(data.iloc[i]["bookofthebible"]),
                "aifirstgrader": str(data.iloc[i]["aifirstgrader"]) if "aifirstgrader" in data.iloc[i] else '',
                "aisimple": str(data.iloc[i]["aisimple"]),
                "aielegant": str(data.iloc[i]["aielegant"]),
                "aicreative": str(data.iloc[i]["aicreative"]),
                "aibiblical": str(data.iloc[i]["aibiblical"]),
                "aicasual": str(data.iloc[i]["aicasual"]),
                "aiformal": str(data.iloc[i]["aiformal"]),
                "ainewsanchor": str(data.iloc[i]["ainewsanchor"]),
                "ailoving": str(data.iloc[i]["ailoving"]),
                "importantphrase": str(data.iloc[i]["importantphrase"]),
                "christiantopics": str(data.iloc[i]["christiantopics"]),
                "biblicalconcepts": str(data.iloc[i]["biblicalconcepts"]),
                "describingwords": str(data.iloc[i]["describingwords"]),
                "biblereferences": str(data.iloc[i]["biblereferences"]),
                "biblephrases": str(data.iloc[i]["biblephrases"]),
                "aiphdstudent": str(data.iloc[i]["aiphdstudent"]),
                "booknumber": bn,
                "episodetitle": str(data.iloc[i]["episodetitle"]),
                "filename": str(data.iloc[i]["filename"]),
                "paragraph": str(data.iloc[i]["paragraph"]),
                "summary": str(data.iloc[i]["summary"]),
                "productionimage": str(data.iloc[i]["productionimage"]),
                "publisher": str(data.iloc[i]["publisher"]),
                "publisherimage": str(data.iloc[i]["publisherimage"]),
                "testament": str(data.iloc[i]["testament"]),
                "type": str(data.iloc[i]["type"]),
                "booktitle": str(data.iloc[i]["booktitle"]),
                "booksubtitle": str(data.iloc[i]["booksubtitle"]),
                "closest": str(data.iloc[i]["closest"]),
                "month": str(data.iloc[i]["month"]),
                "year": str(data.iloc[i]["year"]),
                "isbn": str(data.iloc[i]["isbn"]),
                "author": str(data.iloc[i]["author"]),
                "pages": str(data.iloc[i]["pages"]),
                "journal": str(data.iloc[i]["journal"]),
                "ext1": str(data.iloc[i]["ext1"]),
                "ext2": str(data.iloc[i]["ext2"]),
                "ext3": str(data.iloc[i]["ext3"]),
                "ext4": str(data.iloc[i]["ext4"]),
                "ext5": str(data.iloc[i]["ext5"]),
                "public": "",
                "prodimage": "",
                "prodname": ""
            }
            try:
                client.batch.add_data_object(obj, "NewDocument")
                print(str(i+1)+"/"+str(len(data))+" Completed")
            except:
                print("object at "+str(i)+" Failed")
client.batch.flush()