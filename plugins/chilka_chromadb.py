#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:chilka_chromadb.py - ChromaDB plugin>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""

import pysqlite3
import sys

# Optional hack if you get the sqlite3 version error
sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")

import chromadb
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from chromadb.config import DEFAULT_TENANT, Settings

import spacy

from collections.abc import Iterator
from pathlib import Path

from plugins.chilka_plugin_api import CorpusClientImplAPI

from typing import Any


# Custom data class to format data returned from database
class CustomDataClass():
    """
    An iterable data class with optional db-specific payload
    """

    def __init__(self, data:Any) -> Any:
        """Initialize with data in db-specific format"""
        self._payload = data

    @property
    def payload(self):
        return self._payload

    @payload.setter
    def payload(self, value):
        """Setting the payload property publicly is invalid"""
        raise AttributeError
        
    @payload.deleter
    def payload(self):
        """Deleting the payload property publicly is invalid"""
        raise AttributeError

    def __iter__(self):
        """Implement format conversion and make it iterable"""
        sent_list = self.payload['documents']
        # if payload is from search, we have a nested list
        # strip the outer square brackets
        if type(sent_list[0]) == type([]):
            sent_list = sent_list[0]
        
        n_list_tmp = self.payload['metadatas']
        # if payload is from search, we have a nested list
        # strip the outer square brackets
        if type(n_list_tmp[0]) == type([]):
            n_list_tmp = n_list_tmp[0]
            
        n_list = [mdict['n'] for mdict in n_list_tmp]
        #ids_list = self.payload['ids']
        concat_list =  list(zip(n_list,sent_list))
        # Return generator
        return ({'n':n,'sent':text} for n,text in concat_list)


# Create custom embedding function needed for chromadb semantic search

nlp = spacy.load("en_core_web_md")

class SpacyEmbeddingFunction(EmbeddingFunction):
    def __call__(self, texts:Documents) -> Embeddings:
        return [nlp(text).vector for text in texts]
    
    
class CorpusClientImpl(CorpusClientImplAPI):
    """Implementation of Chilka hooks via plugin implementation api."""
    
    def __init__(self,db_name,connection_string,plugin_args={}):
        """Init method to accept database details.
        
        Args:
            db_name(str): The name of the database/corpus
            connection_string(str): In this case, the persistence folder
            db_plugin(str): The name of the database plugin
        Returns:
            A corpus client object
        """
        
        if plugin_args:
            # Process plugin specific arguments
            pass
        
        self.client = chromadb.PersistentClient(
            path = connection_string+'/'+db_name,
            settings=Settings(persist_directory=connection_string),
            tenant=DEFAULT_TENANT,
            #database="books",
            )

    def add_impl(self,filepath:str,plugin_args={}) -> list:
        """Add file to corpus."""
        
        with open(filepath,'r') as f:
            s = f.read()
        
        collection_name = Path(filepath).name
        
        # Create a collection
        docname = self.client.get_or_create_collection(
            name=collection_name,
            embedding_function=SpacyEmbeddingFunction()
        )
    
        sent_list = [str(s) for s in (nlp(s).sents)]
        index = [str(i) for i in range(1,len(sent_list)+1)]
        mdata = [{'n':i} for i in range(1,len(sent_list)+1)]
        
        
        # Adding documents to the collection
        docname.upsert(
            documents = sent_list,
            ids = index,
            metadatas = mdata
            )
        items = docname.get(include=["documents"])
        return items['ids']


    def remove_impl(self,filename:str,plugin_args={}) -> bool:
        """Remove collection corresponding to filename."""
        
        try:
            self.client.delete_collection(name=filename)
            return filename not in [obj.name for obj in self.client.list_collections()]
        except:
            return True
    
    
    def list_impl(self,plugin_args={}) -> list:
        """List collections/files in database"""
        
        return [obj.name for obj in self.client.list_collections()]


    def readBlob_impl(self,filename:str,plugin_args={}) -> str:
        """Return filename as a text blob"""
        
        col = self.client.get_collection(filename)
        sent_list = col.get(include=['documents'])['documents']
        
        return " ".join(sent_list)


    def readSents_impl(self,filename:str, range_filter=None, kw_filter=None, 
                       plugin_args={"semantic_kw":"","n_results":3}) -> Iterator:
        """Read sentences from specific corpus file using various filters"""
        
        col = self.client.get_collection(filename,embedding_function=SpacyEmbeddingFunction())
        
        if all([range_filter == None,kw_filter == None, plugin_args.get("semantic_kw","") == ""]):
            data_from_db = col.get(include=['documents','metadatas'])
            return CustomDataClass(data_from_db)
       
        #---------------------------------------
        arg_semantic_kw = plugin_args.get('semantic_kw',"")
        arg_n_results = plugin_args.get('n_results',3)
        
        base_partial = "col.query(include=['documents','metadatas'],"+\
            f"query_texts=['{arg_semantic_kw}'],n_results={arg_n_results},"
        
        if kw_filter != None:
            fragment = f"where_document={{'$contains':'{kw_filter}'}},"
            base_partial += fragment
            
        if range_filter != None:
            fragment = f"where={{'$and':[{{'n':{{'$lte':{max(range_filter)}}}}},{{'n':{{'$gte':{min(range_filter)}}}}}]}}"
            base_partial += fragment
        
        final_func_string = base_partial+")"

        data_from_db = eval(f"{final_func_string}")

        return CustomDataClass(data_from_db)

if __name__ == "__main__":

    # Instantiate the corpus client
    my_corpus = CorpusClientImpl("books","../chromadbs1")
    print(my_corpus)

