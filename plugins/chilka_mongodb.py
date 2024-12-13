# Plugin for MongoDB

from collections.abc import Iterator
from pymongo.collection import Collection
from pymongo import MongoClient, ASCENDING
from nltk.tokenize import sent_tokenize
from pathlib import Path

from plugins.chilka_plugin_api import CorpusClientImplAPI


class CorpusClientImpl(CorpusClientImplAPI):
    """Concrete class implementing the corpus API.
    
    Methods:
        add_impl(): Add a file to the corpus
        remove_impl(): Remove a file from the corpus
        readSents_impl(): Read a file stored in the corpus as sentences
        readBlob_impl(): Read a file stored in the corpus as text blob
        list_impl(): List the files in the corpus
    """
    
    def __init__(self,db_name,connection_string,plugin_args={}):
        """Init method to accept database details.
        
        Args:
            db_name(str): The name of the database/corpus
            connection_string(str): The address & port of the database server
            db_plugin(str): The name of the database plugin
        Returns:
            A corpus client object
        """
        
        if plugin_args:
            # Process plugin specific arguments
            pass
        
        # default "mongodb://localhost:27017/"
        self.client = MongoClient(connection_string)
        print(db_name)
        self.db = self.client[db_name]
    
    def add_impl(self,filepath:str,plugin_args={}) -> list:
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
        
    
    def remove_impl(self,filename:str,plugin_args={}) -> bool:
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
        
    
    def readSents_impl(self,filename:str,range_filter=None,kw_filter=None,plugin_args={}) -> Iterator:
        """Returns a file as an iterator of {n:<>,sent:<>} dictionaries
        
        Args:
            filename(str): The name of the file/collection to be read
            range_filter(tuple): (optional)Range of lines to read
            kw_filter(str): (optional)Search term to return sentences containing it
        returns:
            iterator: An iterator of dictionaries containing sentences from the file
            with serial number starting from 1
        """
        
        # No filters, return all the sentences
        if all([range_filter == None, kw_filter == None]):
            return self.db[filename].find({}).sort('n',ASCENDING)
        
        # ...else apply range filter first, then kw filter
        self.cmd_dict = {}
        
        if range_filter != None:
            self.cmd_dict['n'] = {'$gte':min(range_filter),'$lte':max(range_filter)}
            
        if kw_filter != None:
            self.cmd_dict['sent'] = {"$regex":kw_filter}
            
        return self.db[filename].find(self.cmd_dict).sort('n',ASCENDING)
    

    def readBlob_impl(self,filename:str,plugin_args={}) -> str:
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
        
    
    def list_impl(self,plugin_args={}) -> list:
        """List the files in the corpus
        
        Args:
            None
        returns:
            list(str): A list containing filenames in the corpus
        """

        if plugin_args:
            print([item for item in plugin_args.items()])
            
        return self.db.list_collection_names()
