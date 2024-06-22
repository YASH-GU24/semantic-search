import weaviate  # to communicate to the Weaviate instance
import os
from weaviate import Client
from dotenv import load_dotenv
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

# creating the schema
data_schema = {
    "class": "NewDocument",
    "description": "A class called Document",
    "vectorizer": "text2vec-openai",
      "moduleConfig": {
        "text2vec-openai": {
          "model": "text-embedding-3-small",
          "type": "text",
        }
      },
    "properties": [
        {
            "dataType": ["number"],
            "description": "id of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "index",
        },
        {
            "dataType": ["string"],
            "description": "holder of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "holder",
        },
        {
            "dataType": ["string"],
            "description": "Production of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "production",
        },
        {
            "dataType": ["string"],
            "description": "Episodetitle of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "episodetitle",
        },
        {
            "dataType": ["number"],
            "description": "EpisodeNumber of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "episodenumber",
        },
        {
            "dataType": ["number"],
            "description": "part of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "part",
        },
        {
            "dataType": ["text"],
            "description": "paragraph of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "paragraph",
        },
        {
            "dataType": ["text"],
            "description": "summary of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "summary",
        },
        {
            "dataType": ["string"],
            "description": "AiTitle of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aititle",
        },
        {
            "dataType": ["string"],
            "description": "AiSubtitle of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aisubtitle",
        },
        {
            "dataType": ["string"],
            "description": "AiKeywords of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aikeywords",
        },
        {
            "dataType": ["string"],
            "description": "BibleVerses of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "bibleverse",
        },
        {
            "dataType": ["string"],
            "description": "BibleCharacters of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "biblecharacters",
        },
        {
            "dataType": ["string"],
            "description": "BibleConcepts of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "bibleconcepts",
        },
        {
            "dataType": ["string"],
            "description": "FamousPeople of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "famouspeople",
        },
        {
            "dataType": ["string"],
            "description": "BooksMentioned of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "booksmentioned",
        },
        {
            "dataType": ["string"],
            "description": "LifeIssues of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "lifeissues",
        },
        {
            "dataType": ["string"],
            "description": "BiblicalLesson of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "biblicallesson",
        },
        {
            "dataType": ["string"],
            "description": "QuestionAnswered of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "questionanswered",
        },
        {
            "dataType": ["string"],
            "description": "BookOfTheBible of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "bookofthebible",
        },
        {
            "dataType": ["string"],
            "description": "ChristianTopics of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "christiantopics",
        },
        {
            "dataType": ["string"],
            "description": "BiblicalConcepts of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "biblicalconcepts",
        },
        {
            "dataType": ["string"],
            "description": "DescribingWords of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "describingwords",
        },
        {
            "dataType": ["string"],
            "description": "BibleReferences of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "biblereferences",
        },
        {
            "dataType": ["string"],
            "description": "BiblePhrases of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "biblephrases",
        },
        {
            "dataType": ["string"],
            "description": "AllPhdStudents of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aiphdstudent",
        },
        {
            "dataType": ["number"],
            "description": "booknumber of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "booknumber",
        },
        {
            "dataType": ["string"],
            "description": "ProductionImage of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "productionimage",
        },
        {
            "dataType": ["string"],
            "description": "PublisherImage of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "publisherimage",
        },
        {
            "dataType": ["string"],
            "description": "Publisher of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "publisher",
        },
        {
            "dataType": ["string"],
            "description": "testament of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "testament",
        },
        {
            "dataType": ["string"],
            "description": "type of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "type",
        },
        {
            "dataType": ["string"],
            "description": "filename of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
            },
            "name": "filename",
        },
        {
            "dataType": ["string"],
            "description": "importantphrase of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "importantphrase",
        },
        {
            "dataType": ["string"],
            "description": "ailoving of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "ailoving",
        },
        {
            "dataType": ["string"],
            "description": "ainewsanchor of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "ainewsanchor",
        },
        {
            "dataType": ["string"],
            "description": "aiformal of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aiformal",
        },
        {
            "dataType": ["string"],
            "description": "aicasual of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aicasual",
        },
        {
            "dataType": ["string"],
            "description": "aibiblical of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aibiblical",
        },
        {
            "dataType": ["string"],
            "description": "aicreative of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aicreative",
        },
        {
            "dataType": ["string"],
            "description": "aielegant of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aielegant",
        },
        {
            "dataType": ["string"],
            "description": "aisimple of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aisimple",
        },
        {
            "dataType": ["string"],
            "description": "aifirstgrader of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "aifirstgrader",
        },
        {
            "dataType": ["string"],
            "description": "booktitle of the object",
            "moduleConfig": {
                "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
            },
            "name": "booktitle",
        },
    {
        "dataType": ["string"],
        "description": "booksubtitle of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "booksubtitle"
    },
    {
        "dataType": ["string"],
        "description": "closest of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "closest"
    },
    {
        "dataType": ["string"],
        "description": "month of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
        },
        "name": "month"
    },
    {
        "dataType": ["string"],
        "description": "year of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
        },
        "name": "year"
    },
    {
        "dataType": ["string"],
        "description": "isbn of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
        },
        "name": "isbn"
    },
    {
        "dataType": ["string"],
        "description": "author of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
        },
        "name": "author"
    },
    {
        "dataType": ["string"],
        "description": "pages of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": True, "vectorizePropertyName": False}
        },
        "name": "pages"
    },
    {
        "dataType": ["string"],
        "description": "journal of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "journal"
    },
    {
        "dataType": ["string"],
        "description": "ext1 of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "ext1"
    },
    {
        "dataType": ["string"],
        "description": "ext2 of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "ext2"
    },
    {
        "dataType": ["string"],
        "description": "ext3 of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "ext3"
    },
    {
        "dataType": ["string"],
        "description": "ext4 of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "ext4"
    },
    {
        "dataType": ["string"],
        "description": "ext5 of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "ext5"
    },
    {
        "dataType": ["string"],
        "description": "public of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "public"
    },
    {
        "dataType": ["string"],
        "description": "prodimage of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "prodimage"
    },
    {
        "dataType": ["string"],
        "description": "prodname of the object",
        "moduleConfig": {
            "text2vec-openai": {"skip": False, "vectorizePropertyName": True}
        },
        "name": "prodname"
    }
    ],
    "vectorizer": "text2vec-openai",
}

# creating the schema
current_schemas = client.schema.get()["classes"]
for schema in current_schemas:
    if schema["class"] == "NewDocument":
        client.schema.delete_class("NewDocument")
client.schema.create_class(data_schema)
