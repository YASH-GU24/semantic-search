# Importing required packages
import weaviate # to communicate to the Weaviate instance
import time
from weaviate import Client
import pandas as pd
#setting up client
client  = weaviate.Client(
    url="https://documents.semi.network",
    additional_headers={
        'X-OpenAI-Api-Key': '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'
    }
)



#opening the datafile
#Enter Path Of Csv File below
data=pd.read_csv("column_titles.csv")



#creating the schema
data_schema={
      "class": "Document_Extended",
      "description": "A class called document",
      "vectorizer": "text2vec-openai",
      "moduleConfig": {
        "text2vec-openai": {
          "model": "ada",
          "modelVersion": "002",
          "type": "text"
        }
      },
      "properties": [
          {
          "dataType": [
            "number"
          ],
          "description": "id of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "index"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "Filename of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "Filename"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "Production of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "Production"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "EpisodeTitle of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "EpisodeTitle"
        },
        {
          "dataType": [
            "number"
          ],
          "description": "EpisodeNumber of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "EpisodeNumber"
        },
        {
          "dataType": [
            "number"
          ],
          "description": "part of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "part"
        },
        {
          "dataType": [
            "text"
          ],
          "description": "paragraph of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "paragraph"
        },
        {
          "dataType": [
            "text"
          ],
          "description": "summary of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "summary"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "AiTitle of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "AiTitle"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "AiSubtitle of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "AiSubtitle"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "AiKeywords of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "AiKeywords"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BibleVerses of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BibleVerses"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BibleCharacters of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BibleCharacters"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BibleConcepts of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BibleConcepts"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "FamousPeople of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "FamousPeople"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BooksMentioned of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BooksMentioned"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "LifeIssues of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "LifeIssues"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BiblicalLesson of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BiblicalLesson"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "QuestionAnswered of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "QuestionAnswered"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BookOfTheBible of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BookOfTheBible"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "ImportantPhrase of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "ImportantPhrase"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "ChristianTopics of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "ChristianTopics"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BiblicalConcepts of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BiblicalConcepts"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "DescribingWords of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "DescribingWords"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BibleReferences of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BibleReferences"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "BiblePhrases of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "BiblePhrases"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "AllPhdStudents of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "AiPhdStudents"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "ProductionImage of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "ProductionImage"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "PublisherImage of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "PublisherImage"
        },
        {
          "dataType": [
            "string"
          ],
          "description": "Publisher of the object",
          "moduleConfig": {
            "text2vec-openai": {
              "skip": False,
              "vectorizePropertyName": False
            }
          },
          "name": "Publisher"
        }
      ],
      "vectorizer": "text2vec-openai"
    }

# creating the schema 
current_schemas = client.schema.get()['classes']
for schema in current_schemas:
    if schema['class']=='Document_Extended':
        client.schema.delete_class('Document_Extended')
client.schema.create_class(data_schema)

#configuring batch import for weaviate
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

#importing the data objects
for i in range (0,len(data)):
    obj = {
                "index": int(data.iloc[i]['id']) if 'id' in data.iloc[i] else '',
                "FileName":str(data.iloc[i]['Filename']) if 'Filename' in data.iloc[i] else '',
                "Production":str(data.iloc[i]['Production']) if 'Production' in data.iloc[i] else '',
                "EpisodeTitle":str(data.iloc[i]['Episode Title']) if 'Episode Title' in data.iloc[i] else '',
                "EpisodeNumber":int(data.iloc[i]['Episode Number']) if 'Episode Number' in data.iloc[i] else '',
                "Part": int(data.iloc[i]['Part']) if 'Part' in data.iloc[i] else '',
                "Paragraph":str(data.iloc[i]['Paragraph']) if 'Paragraph' in data.iloc[i] else '',
                "Summary": str(data.iloc[i]['Summary']) if 'Summary' in data.iloc[i] else '',
                "AiTitle": str(data.iloc[i]['AI Title']) if 'AI Title' in data.iloc[i] else '',
                "AiSubtitle": str(data.iloc[i]['AI Subtitle']) if 'AI Subtitle' in data.iloc[i] else '',
                "AiKeywords":str(data.iloc[i]['AI Keywords']) if 'AI Keywords' in data.iloc[i] else '',
                "BibleVerses": str(data.iloc[i]['Bible Verses']) if 'Bible Verses' in data.iloc[i] else '',
                "BibleCharacters":str(data.iloc[i]['Bible Characters']) if 'Bible Characters' in data.iloc[i] else '',
                "BibleConcepts": str(data.iloc[i]['Bible Concepts']) if 'Bible Concepts' in data.iloc[i] else '',
                "FamousPeople":str(data.iloc[i]['Famous People']) if 'Famous People' in data.iloc[i] else '',
                "BooksMentioned": str(data.iloc[i]['Books Mentioned']) if 'Books Mentioned' in data.iloc[i] else '',
                "LifeIssues": str(data.iloc[i]['Life Issues']) if 'Life Issues' in data.iloc[i] else '',
                "BiblicalLesson":str(data.iloc[i]['Biblical Lesson']) if 'Biblical Lesson' in data.iloc[i] else '',
                "QuestionAnswered":str(data.iloc[i]['Questions Answered']) if 'Questions Answered' in data.iloc[i] else '',
                "BookOfTheBible": str(data.iloc[i]['Book of the Bible']) if 'Book of the Bible' in data.iloc[i] else '',
                "ImportantPhrase":str(data.iloc[i]['Important Phrase']) if 'Important Phrase' in data.iloc[i] else '',
                "ChristianTopics": str(data.iloc[i]['Christian Topics']) if 'Christian Topics' in data.iloc[i] else '',
                "BiblicalConcepts":str(data.iloc[i]['Biblical Concepts']) if 'Biblical Concepts' in data.iloc[i] else '',
                "DescribingWords": str(data.iloc[i]['Describing Words']) if 'Describing Words' in data.iloc[i] else '',
                "BibleReferences":str(data.iloc[i]['Bible References']) if 'Bible References' in data.iloc[i] else '',
                "BiblePhrases": str(data.iloc[i]['Bible Phrases']) if 'Bible Phrases' in data.iloc[i] else '',
                "AiPhdStudents":str(data.iloc[i]['AI PHD Student']) if 'AI PHD Student' in data.iloc[i] else '',
                "ProductionImage": str(data.iloc[i]['Production Image']) if 'Production Image' in data.iloc[i] else '',
                "PublisherImage":str(data.iloc[i]['Publisher Image']) if 'Publisher Image' in data.iloc[i] else '',
                "Publisher": str(data.iloc[i]['Publisher']) if 'Publisher' in data.iloc[i] else ''
                
          }
    #checking the items imported
    print(str(i+1)+"/"+str(len(data))+" Completed")
    try:
      client.batch.add_data_object(obj, "Document_Extended")
    except:
      print("object at "+i+" Failed","failed")
client.batch.flush()
