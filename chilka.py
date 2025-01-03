#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Chilka is a corpus serving library with a basic sensible
interface and a pluggable backend to accomodate different
databases.

Chilka implements the following interface:
    
    - `add()`: Add a file to the corpus.
    - `remove()`: Remove a file from the corpus.
    - `list()`: List files from the corpus.
    - `readSents()`: Read sentences of a particular file based on conditions.
    - `readBlob()`: Get entire file as a text blob.
    
The plugin implementation lets you implement and enforce your own schema.
The `plugin_args` argument lets you pass custom arguments to your plugin.
"""

"""
# <Chilka:chilka.py - Corpus creation library>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""

import abc
from collections.abc import Iterator
import importlib


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
    def __init__(self,db_name:str,connection_string:str,db_plugin=None,plugin_args={}):
        """Init method to accept database details.
        
        Args:
            db_name (str): The name of the database/corpus
            connection_string (str): The address & port of the database server
            db_plugin (str): The name of the database plugin
        Returns:
            A corpus client object
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def add(self,filepath:str,plugin_args={}) -> list:
        """Adds a file to the file list
        
        Args:
            filepath (str): The path of the file to add to the corpus
        Returns:
            list: The list of IDs of objects added
        
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def remove(self,filename:str,plugin_args={}) -> bool:
        """Removes a file from the corpus
        
        Args:
            filename (str): The name of the file to be removed from the corpus
        Returns:
            bool: True if the collection was removed successfully, false if it
            does not exist
        """

        raise NotImplementedError
    
    @abc.abstractmethod
    def readSents(self,filename:str,range_filter:tuple=None,kw_filter:str=None,plugin_args={}) -> Iterator:
        """Returns a file as an iterator of {n:<>,sent:<>} dictionaries
        
        Args:
            filename (str): The name of the file to be read
            range_filter (tuple): (optional)Range of lines to read
            kw_filter (str): (optional)Search term to return sentences containing it
        returns:
            iterator: An iterator of dictionaries containing sentences from the file
            with serial number starting from 1
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def readBlob(self,filename:str,plugin_args={}) -> str:
        """Reads a file as a text blob
        
        Args:
            filename (str): The name of the file to be read
        returns:
            str: File content as a single string
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def list(self,plugin_args={}) -> list:
        """List the files in the corpus
        
        Args:
            None
        returns:
            list (str): A list containing filenames in the corpus
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
    
    def __init__(self,db_name:str,connection_string:str,db_plugin=None,plugin_args={}):
        """Init method to accept database details.
        
        Args:
            db_name (str): The name of the database/corpus
            connection_string (str): The address & port of the database server
            db_plugin (str): The name of the database plugin
        Returns:
            A corpus client object
        """
        
        # Load the plugin
        plugin_path = "plugins.chilka_" + db_plugin
        plugin = importlib.import_module(plugin_path)
        
        # Instantiate the plugin client
        self.pu_client = plugin.CorpusClientImpl(db_name, connection_string,
                                               plugin_args=plugin_args)
        
        
    def add(self,filepath:str,plugin_args={}) -> list:
        """Adds a file to the file list
        
        Args:
            filepath (str): The path of the file to add to the corpus
        Returns:
            list: The list of IDs of objects added
        
        """
        
        #-----
        return self.pu_client.add_impl(filepath, plugin_args=plugin_args)
        #-----

    
    def remove(self,filename:str,plugin_args={}) -> bool:
        """Removes a file from the corpus
        
        Args:
            filename (str): The name of the file to be removed from the corpus
        Returns:
            bool: True if the collection was removed successfully, false if it
            does not exist
        """
        
        #------
        return self.pu_client.remove_impl(filename,plugin_args=plugin_args)
        #------
        
    
    def readSents(self,filename:str,range_filter=None,kw_filter=None,plugin_args={}) -> Iterator:
        """Returns a file as an iterator of {n:<>,sent:<>} dictionaries
        
        Args:
            filename (str): The name of the file/collection to be read
            range_filter (tuple): (optional)Range of lines to read
            kw_filter (str): (optional)Search term to return sentences containing it
        returns:
            iterator: An iterator of dictionaries containing sentences from the file
            with serial number starting from 1
        """
        
        return self.pu_client.readSents_impl(filename,range_filter=range_filter,
                                             kw_filter=kw_filter,
                                             plugin_args=plugin_args)
    

    def readBlob(self,filename:str,plugin_args={}) -> str:
        """Reads a file as a text blob
        
        Args:
            filename (str): The name of the file to be read
        returns:
            str: File content as a single string
        """

        return self.pu_client.readBlob_impl(filename, plugin_args=plugin_args)
    
    
    def list(self,plugin_args={}) -> list:
        """List the files in the corpus
        
        Args:
            None
        returns:
            list (str): A list containing filenames in the corpus
        """
        #return self.db.list_collection_names()
        # Use the plugin reference to get list of filenames
        
        return self.pu_client.list_impl(plugin_args = plugin_args)
        
