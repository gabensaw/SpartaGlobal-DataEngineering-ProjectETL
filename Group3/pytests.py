import pandas as pd

from main import *
import pytest


def test_type():
    assert type(getting_text_file_names()) == type(list([])) # "Should be list"

def test_file_type():
    test_files = getting_text_file_names()
    assert type(test_files[0]) == type('') # "Should be txt"

def test_txt():
    test_files = getting_text_file_names()
    for i in range(len(test_files)):
        test_files[i].endswith('.txt')
        assert True

def test_academy():
    test = getting_text_file_names()
    extract = extract_info(test)
    for i in extract['Academy']:
        assert type(i) is str

def test_fullname():
    test = getting_text_file_names()
    extract = extract_info(test)
    for i in extract['FullName']:
        assert type(i) is str

def test_psy():
    test = getting_text_file_names()
    extract = extract_info(test)
    for i in extract['Psychometrics']:
        assert type(i) is str or float



