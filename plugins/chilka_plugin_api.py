#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:chilka_plugin_api.py - Chilka API plugin interface>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""

import abc
from collections.abc import Iterator


class NotImplementedError(BaseException):
    # Raised when an unimplemented base class method is called
    pass


class ServiceUnavailableError(BaseException):
    # Raised when a service is not available
    pass


class CorpusClientImplAPI(metaclass=abc.ABCMeta):
    """Abstract base class implementing the corpus API hooks.
    
    Methods:
        add_impl(): Add a file to the corpus
        remove_impl(): Remove a file from the corpus
        readSents_impl(): Read a file stored in the corpus as sentences
        readBlob_impl(): Read a file stored in the corpus as text blob
        list_impl(): List the files in the corpus
    """
    @abc.abstractmethod
    def __init__(self,db_name:str,connection_string:str,db_plugin=None,plugin_args={}):
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
    def add_impl(self,filepath:str,plugin_args={}) -> list:
        """Adds a file to the file list
        
        Args:
            filepath(str): The path of the file to add to the corpus
        Returns:
            list: The list of IDs of objects added
        
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def remove_impl(self,filename:str,plugin_args={}) -> bool:
        """Removes a file from the corpus
        
        Args:
            filename(str): The name of the file to be removed from the corpus
        Returns:
            bool: True if the collection was removed successfully, false if it
            does not exist
        """

        raise NotImplementedError
    
    @abc.abstractmethod
    def readSents_impl(self,filename:str,range_filter:tuple=None,kw_filter:str=None,plugin_args={}) -> Iterator:
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
    def readBlob_impl(self,filename:str,plugin_args={}) -> str:
        """Reads a file as a text blob
        
        Args:
            filename(str): The name of the file to be read
        returns:
            str: File content as a single string
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
    def list_impl(self,plugin_args={}) -> list:
        """List the files in the corpus
        
        Args:
            None
        returns:
            list(str): A list containing filenames in the corpus
        """

        raise NotImplementedError
