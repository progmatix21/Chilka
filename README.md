# Chilka

Chilka is a corpus server library with a document
database backend -- currently MongoDB.

## Features

- Easy interface that hides the complexity of the
database.
- Direct ingestion of text files helps automate your
corpus generation task.
- Flexible read interface that lets you retrieve documents:
	- by sentence number range.
	- by keyword filtering.
	- combination of both of the above.
	- as a text blob.
- Simple database schema allows you to access the database
directly if necessary.
- Database backend lets your corpus scale and also grants
ease of access.

## Schema

Chilka sentencizes a text document and stores the sentences
as individual database documents using the following schema:

```
	{'n' : <sentence-number>, 'sent' : <sentence-text>}
```

Each file name gets its own collection of the same name.


## Example code

Get going with just a few lines of code.
```python
from chilka import CorpusClient
from pprint import pprint

# Assumes a MongoDB instance is running on localhost.  Replace with your
# connection string if this is not applicable.
# Connect to the MongoDB server and create a 'corpus' database
# Will get reference to a corpus DB if it already exists
my_corpus = CorpusClient("corpus", "mongodb://localhost:27017/")

# List all the collections present in the db, one collection per file
print("-" * 79)
print(f"List of collections in DB: {my_corpus.list()}")
print("-" * 79)

# Add files to the DB
filefolder = "./Text/"
for filename in ["mayon_volcano.txt","ukraine_dam.txt"]:
    pprint(f"Add file {filename}:\n{my_corpus.add(f'{filefolder}'+f'{filename}')}")
    print("-"*40)
    
# List all the collections present in the DB
print(f"List of collections/files in DB: {my_corpus.list()}")
print("-" * 79)

# List sentences in each collection using filters.  Gives object IDs and sentence numbers
# as well.  Extract the 'sent' key to get just the sentences.
print("Sentences with the word 'sun' in file mayon_volcano.txt:")
pprint(f"{list(my_corpus.readSents('mayon_volcano.txt', range_filter=None,kw_filter='Sun.+'))}")
print("-" * 79)
print("Sentences in the range(15,20) in file mayon_volcano.txt:")
pprint(f"{list(my_corpus.readSents('mayon_volcano.txt', range_filter=(15,20),kw_filter=None))}")
print("-" * 79)

print(f"Sentences in the form of a text blob:\n {my_corpus.readBlob('ukraine_dam.txt')}")
print("-" * 79)

# Remove unwanted collections from DB
print(f"Remove collection ukraine_dam: {my_corpus.remove('ukraine_dam.txt')}")
print("-" * 79)

# List all collections in the DB
print(f"List of collections in DB: {my_corpus.list()}")

```

## Installation
Use the provided `requirements.txt` with pip to create the installation environment.  Then, copy or clone
this repository to start using Chilka.