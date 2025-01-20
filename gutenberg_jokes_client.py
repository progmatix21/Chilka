#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:gutenberg_jokes_client.py - Demo of Chilka corpus library with serverless chromadb>
# Copyright (C) <2025>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""
from chilka import CorpusClient
import random

"""
This demo client uses the plugin chilka_chromadb_gutenberg_jokes.py, 
for the serverless mode of chromadb.  While the semantics of the database name 
remain the same, the database persistence folder becomes the 'connection string'.

Chromadb in the serverless mode uses sqlite3 as the default backend.

Chromadb being a vector database, has a semantic search feature as well.  We
exploit this feature by passing a 'semantic_kw' argument as we show later.
"""

# Use Chilka corpus client to create your corpus object
my_corpus = CorpusClient("gutenberg_jokes", "./chromadbs",db_plugin="chromadb_gutenberg_jokes")


# The following functions implement menu commands for this client
def list_matching_sentences(searchword):
    # Conducts a semantic search based on a keyword
    
    
    kw_sent_list = list(my_corpus.readSents('jokes.txt',
                        range_filter=None,
                        #kw_filter=searchword,
                        plugin_args={'semantic_kw':searchword}))
    
    return (joke['sent'] for joke in kw_sent_list)
    

def range_sents(sentrange):
    # Print jokes in a specific range.  Remember to give 'n_results' >= line count.
    # Line range includes both limits.
    
    sentrange = input("Enter joke range: ").split()
    if len(sentrange) == 2:
        range_tuple = tuple(sentrange)
    else:
        exit()
    print(f"Sentences in the range {range_tuple} in joke file:")
    range_of_sents = list(my_corpus.readSents('jokes.txt', range_filter=range_tuple,
                                          plugin_args={'n_results':6}))
    # Sort the sentences on line number
    range_of_sents.sort(key = lambda x: x['n'])
    return [j['sent'] for j in range_of_sents]

def add_joke_file(filename):
    # Add files to the DB
    filefolder = "./Text/"
    list_added = []
    for filename in ["jokes.txt"]:
        joke_ids = my_corpus.add(f'{filefolder}'+f'{filename}')
        list_added.append((filename,len(joke_ids)))
        #pprint(f"Add file {filename}:\n{my_corpus.add(f'{filefolder}'+f'{filename}')}")
    return list_added


def list_sent_blob(filename):
    print(f"Sentences in the form of a text blob:\n {my_corpus.readBlob('jokes.txt')}")

def random_joke():
    rand_joke = random.choice(list(my_corpus.readSents('jokes.txt',
                                    range_filter=None,kw_filter=None)))
    return rand_joke['sent']
    

# Print the menu in the command loop
while True:
    
    print("-----")
    print("Chilka Gutenberg jokes corpus:\n")
    print("1. List collections")
    print("2. Add file to the corpus")
    print("3. Search joke by semantic keyword")
    print("4. Get jokes by numeric range")
    print("5. Get a random joke")
    print("6. Exit")
    
    choice = int(input("\nEnter your choice: "))
    
    if choice == 1:
        # List all the collections present in the db, one collection per file
        print(f"List of collections in DB: {my_corpus.list()}")
    
    elif choice == 2:
        # Add files to the DB
        filename = input("Specify name of file to be added: ./Text/")
        print(add_joke_file(filename))
        print()
    
    elif choice == 3:
        # List sentences in each collection using filters.  Gives object IDs and sentence numbers
        # as well.  Extract the 'sent' key to get just the sentences.
        searchword = input("Semantic keyword search: ")
        for joke in list_matching_sentences(searchword):
            print(joke)
            print("**")
        
    elif choice == 4:
        # Print sentences in a specific range.  Remember to give 'n_results' >= line count.
        # Line range includes both limits.
        for joke in range_sents((2,5)):
            print(joke)
            print("****")
    
    elif choice == 5:
        print(random_joke())
    
    elif choice == 6:
        exit()
