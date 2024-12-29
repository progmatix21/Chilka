#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:demo_client2.py - Demo of Chilka corpus library with serverless chromadb>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""
from chilka import CorpusClient
from pprint import pprint

"""
This demo client uses the plugin chilka_chromadb.py, for the serverless mode
of chromadb.  While the semantics of the database name remain the same, the
database persistence folder becomes the 'connection string'.

Chromadb in the serverless mode uses sqlite3 as the default backend.

Chromadb being a vector database, has a semantic search feature as well.  We
exploit this feature by passing a 'semantic_kw' argument as we show later.
"""
my_corpus = CorpusClient("news", "./news_corpus",db_plugin="chromadb")

# List all the collections present in the db, one collection per file
print("-" * 79)
print(f"List of collections in DB: {my_corpus.list()}")
print("-" * 79,"\n")

# Add files to the DB
filefolder = "./Text/"
for filename in ["mayon_volcano.txt","ukraine_dam.txt"]:
    pprint(f"Add file {filename}:\n{my_corpus.add(f'{filefolder}'+f'{filename}')}")
    print("-"*40)
print()


# List all the collections present in the DB
print(f"List of collections/files in DB: {my_corpus.list()}")
print("-" * 79,"\n")

# List sentences in each collection using filters.  Gives object IDs and sentence numbers
# as well.  Extract the 'sent' key to get just the sentences.
print("Sentences with the word 'sunday' in file mayon_volcano.txt:")
kw_sent_list = list(my_corpus.readSents('mayon_volcano.txt',
                                        range_filter=None,kw_filter='Sunday'))
pprint(kw_sent_list)
print("-" * 79,"\n")

# List sentences using semantic search
print("Sentences similar to the query 'cattle' (decreasing order of relevance): ")
sem_sent_list = list(my_corpus.readSents('mayon_volcano.txt',
                    plugin_args={'semantic_kw':'cattle','n_results':5}))
pprint(sem_sent_list)
print("-" * 79,"\n")


# Print sentences in a specific range.  Remember to give 'n_results' >= line count.
# Line range includes both limits.
print("Sentences in the range(15,20) in file mayon_volcano.txt:")
sent_15_20 = list(my_corpus.readSents('mayon_volcano.txt', range_filter=(15,20),
                                      plugin_args={'n_results':6}))
# Sort the sentences on line number
sent_15_20.sort(key = lambda x: x['n'])
pprint(sent_15_20)
print("-" * 79,"\n")

# Print file as text blob
print(f"Sentences in the form of a text blob:\n {my_corpus.readBlob('ukraine_dam.txt')}")
print("-" * 79,"\n")

# Remove unwanted collections from DB
print(f"Remove collection ukraine_dam: {my_corpus.remove('ukraine_dam.txt')}")
print("-" * 79,"\n")

# List all collections in the DB
print(f"List of collections in DB: {my_corpus.list()}")
