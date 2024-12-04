#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:demo_client.py - Demonstrate use of Chilka corpus library>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""
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
