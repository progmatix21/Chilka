#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# <Chilka:test_client.py - Test runner for the Chilka library>
# Copyright (C) <2024>  <Atrij Talgery: github.com/progmatix21>
# SPDX-License-Identifier: AGPL-3.0-or-later
# https://www.gnu.org/licenses/agpl.txt
# https://spdx.org/licenses/AGPL-3.0-or-later.html
"""
from chilka import CorpusClient
import unittest
import tempfile
import shutil

"""
This test client uses the plugin chilka_chromadb.py, for the serverless mode
of chromadb.  While the semantics of the database name remain the same, the
database persistence folder becomes the 'connection string'.

This client runs some basic tests on the library.

Chromadb in the serverless mode uses sqlite3 as the default backend.
"""

class ChromadbTestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Setup for this test fixture.  Runs once per test-case.
        
        cls.db_folder = tempfile.mkdtemp()
        cls.my_corpus = CorpusClient("test_corpus", cls.db_folder,
                         db_plugin="chromadb")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(ChromadbTestCase.db_folder)

    
    def test1_add(self):
        # test function for add_impl()
        # Add files to the DB
        filefolder = "./Text/"
        
        filename = 'mayon_volcano.txt'  # 28 sentences by spacy sentencizer
        self.assertEqual(28,len(ChromadbTestCase.my_corpus.add(f'{filefolder}'+f'{filename}')),
                     "Sentences into database == sentences out of database.")
        filename = 'ukraine_dam.txt'  # 28 sentences by spacy sentencizer
        self.assertEqual(11,len(ChromadbTestCase.my_corpus.add(f'{filefolder}'+f'{filename}')),
                     "Sentences into database == sentences out of database.")
        
        self.assertEqual(2,len(ChromadbTestCase.my_corpus.list()),"num files in corpus")
        
        self.assertIn("mayon_volcano.txt",ChromadbTestCase.my_corpus.list(),"Test file membership.")
        self.assertIn("ukraine_dam.txt",ChromadbTestCase.my_corpus.list(),"Test file membership.")


    def test2_read(self):
        # Test file retrieval
        data_from_db = ChromadbTestCase.my_corpus.readSents("mayon_volcano.txt")
        self.assertEqual(28,len(list(data_from_db)),"Test read from database.")
        self.assertEqual(type({}),type(list(data_from_db)[0]), "Test returned datatype.")

    def test3_read_with_kw(self):
        # Test file retrieval with keyword
        data_from_db = ChromadbTestCase.my_corpus.readSents("mayon_volcano.txt",kw_filter="Sunday")
        self.assertEqual(3,len(list(data_from_db)),"Test read keyword from database.")
        self.assertEqual(type({}),type(list(data_from_db)[0]), "Test returned datatype.")


if __name__ == '__main__':

    unittest.main()
