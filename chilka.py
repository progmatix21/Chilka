#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:chilka.py - Corpus creation library>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""

import abc
from collections.abc import Iterator
from pymongo.collection import Collection
from pymongo import MongoClient, ASCENDING
from nltk.tokenize import sent_tokenize
from pathlib import Path

class NotImplementedError(BaseException):
    # Raised when an unimplemented base class method is called
    pass


class ServiceUnavailableError(BaseException):
    # Raised when a service is not available
    pass


class CorpusClientAPI(metaclass=abc.ABCMeta):
    """Abstract base class defining the corpus API.
    
    Methods:
        add(): Add a file to the corpus
        remove(): Remove a file from the corpus
        readSents(): Read a file stored in the corpus as sentences
        readBlob(): Read a file stored in the corpus as text blob
        list(): List the files in the corpus
    """
    @abc.abstractmethod
    def __init__(self,db_name:str,connection_string:str,db_plugin=None):
        """Init method to accept database details.
        
        Args:
            db_name(str): The name of the database/corpus
            connection_string(str): The address & port of the database server
            db_plugin(str): The name of the database plugin
        Returns:
            A corpus client object
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def add(self,filepath:str) -> list:
        """Adds a file to the file list
        
        Args:
            filepath(str): The path of the file to add to the corpus
        Returns:
            list: The list of IDs of objects added
        
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def remove(self,filename:str) -> bool:
        """Removes a file from the corpus
        
        Args:
            filename(str): The name of the file to be removed from the corpus
        Returns:
            bool: True if the collection was removed successfully, false if it
            does not exist
        """

        raise NotImplementedError
    
    @abc.abstractmethod
    def readSents(self,filename:str,range_filter:tuple=None,kw_filter:str=None) -> Iterator:
        """Returns a file as an iterator of {n:<>,sent:<>} dictionaries
        
        Args:
            filename(str): The name of the file to be read
            range_filter(tuple): (optional)Range of lines to read
            kw_filter(str): (optional)Search term to return sentences containing it
        returns:
            iterator: An iterator of dictionaries containing sentences from the file
            with serial number starting from 1
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def readBlob(self,filename:str) -> str:
        """Reads a file as a text blob
        
        Args:
            filename(str): The name of the file to be read
        returns:
            str: File content as a single string
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def list(self) -> list:
        """List the files in the corpus
        
        Args:
            None
        returns:
            list(str): A list containing filenames in the corpus
        """

        raise NotImplementedError


class CorpusClient(CorpusClientAPI):
    """Concrete class implementing the corpus API.
    
    Methods:
        add(): Add a file to the corpus
        remove(): Remove a file from the corpus
        readSents(): Read a file stored in the corpus as sentences
        readBlob(): Read a file stored in the corpus as text blob
        list(): List the files in the corpus
    """
    
    def __init__(self,db_name,connection_string,db_plugin=None):
        """Init method to accept database details.
        
        Args:
            db_name(str): The name of the database/corpus
            connection_string(str): The address & port of the database server
            db_plugin(str): The name of the database plugin
        Returns:
            A corpus client object
        """
        # default "mongodb://localhost:27017/"
        self.client = MongoClient(connection_string)
        self.db = self.client[db_name]
    
    def add(self,filepath:str) -> list:
        """Adds a file to the file list
        
        Args:
            filepath(str): The path of the file to add to the corpus
        Returns:
            list: The list of IDs of objects added
        
        """
        
        with open(filepath,'r') as f:
            s = f.read()
        
        collection_name = Path(filepath).name
        
        self.db[collection_name].delete_many({}) # Delete old existing collection

        self.collection = Collection(self.db, collection_name, create=True)
        
        # sentencize the text file
        sent_list = sent_tokenize(s)
        
        doc_dict = [{'n':i, 'sent':s} for i,s in enumerate(sent_list,start=1)]
        
        self.filedict_ids = self.collection.insert_many(doc_dict).inserted_ids
        
        # Create an index
        self.collection.create_index(
            [( "sent", "text" )]
        )
        
        return self.filedict_ids
        
    
    def remove(self,filename:str) -> bool:
        """Removes a file from the corpus
        
        Args:
            filename(str): The name of the file to be removed from the corpus
        Returns:
            bool: True if the collection was removed successfully, false if it
            does not exist
        """
        
        colref = self.db[filename]  # Get reference of filename collection
        colref.drop()  # A file has a collection to itself
        
        return filename not in self.db.list_collection_names()
        
    
    def readSents(self,filename:str,range_filter=None,kw_filter=None) -> Iterator:
        """Returns a file as an iterator of {n:<>,sent:<>} dictionaries
        
        Args:
            filename(str): The name of the file/collection to be read
            range_filter(tuple): (optional)Range of lines to read
            kw_filter(str): (optional)Search term to return sentences containing it
        returns:
            iterator: An iterator of dictionaries containing sentences from the file
            with serial number starting from 1
        """
        
        if all([range_filter == None, kw_filter == None]):
            return self.db[filename].find({}).sort('n',ASCENDING)
        
        self.cmd_dict = {}
        
        if range_filter != None:
            self.cmd_dict['n'] = {'$gte':min(range_filter),'$lte':max(range_filter)}
            
        if kw_filter != None:
            self.cmd_dict['sent'] = {"$regex":kw_filter}
            
        return self.db[filename].find(self.cmd_dict).sort('n',ASCENDING)
    

    def readBlob(self,filename:str) -> str:
        """Reads a file as a text blob
        
        Args:
            filename(str): The name of the file to be read
        returns:
            str: File content as a single string
        """

        sent_list = []
        sent_dict_list = list(self.db[filename].find({}).sort('n',ASCENDING))

        for mydict in sent_dict_list:
            sent_list.append(mydict['sent'])
            
        return "  ".join(sent_list)
        
    
    def list(self) -> list:
        """List the files in the corpus
        
        Args:
            None
        returns:
            list(str): A list containing filenames in the corpus
        """
        return self.db.list_collection_names()
        
