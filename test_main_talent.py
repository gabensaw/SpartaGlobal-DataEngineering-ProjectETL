import pytest
from main_talent import *


def test_get_fields(content_type):  # test each case of json key: value extraction
    test_input = {'name': 'Stillmann Castano', 'date': '22/08/2019',
                  'tech_self_score': {'C#': 6, 'Java': 5, 'R': 2, 'JavaScript': 2},
                  'strengths': ['Charisma'],
                  'weaknesses': ['Distracted', 'Impulsive', 'Introverted'],
                  'self_development': 'Yes', 'geo_flex': 'Yes',
                  'financial_support_self': 'Yes', 'result': 'Pass',
                  'course_interest': 'Business'}
    match content_type:
        case 'main':  # test case for main_talent (non-embedded elements) extraction
            assert get_fields(test_input, 'main', 1) == [1, 'Stillmann Castano', '22/08/2019',
                                                         'Yes', 'Yes', 'Yes', 'Pass', 'Business']
        case 'self_scores':  # test case for tech_self_score dict extraction
            assert get_fields(test_input, 'self_scores', 1) == [{'C#': 6, 'Java': 5, 'R': 2, 'JavaScript': 2}]
        case 'strengths': # test case for strengths list extraction
            assert get_fields(test_input, 'strengths', 1) == [['Charisma']]
        case 'weaknesses': # test case for weaknesses list extraction
            assert get_fields(test_input, 'weaknesses', 1) == [['Distracted', 'Impulsive', 'Introverted']]


def test_write_to_csv():
    test = [[1, 'Stillman Castano', '22/08/2018', 'Yes', 'Yes', 'No', 'Pass', 'Business'],
            [2, ' Joe Blogs', '01/08/2019', 'No', 'Yes', 'Yes', 'Fail', 'Data']]
    result = write_to_csv(test, "main")  # Turning the test list_of_lists into a csv.
    assert result == [['id', 'name', 'date', 'self_development', 'geo_flex', 'financial_support_self', 'result',
                       'course_interest'],
                      ['Stillman Castano', '22/08/2018', 'Yes', 'Yes', 'No', 'Pass', 'Business'],
                      ['Joe Blogs', '01/08/2019', 'No', 'Yes', 'Yes', 'Fail', 'Data']]


def test_write_to_csv_weaknesses():
    df_weaknesses = pd.read_csv('weaknesses.csv')  # Turns the weaknesses csv into a data frame
    df_main = pd.read_csv('main_talent.csv') # Turns the main file into a dataframe
    assert df_weaknesses['id'].max() == len(df_main['id'])  # Checks the max id is equal to the number of ids in the main file


def test_write_to_csv_strengths():
    df_strengths = pd.read_csv('strengths.csv')  # Turns the weaknesses csv into a data frame
    df_main = pd.read_csv('main_talent.csv')  # Turns the main file into a dataframe
    assert df_strengths['id'].max() == len(df_main['id'])  # Checks the max id is equal to the number of ids in the main file


def test_write_to_csv_tech_self_score():
    df_tech = pd.read_csv('tech_self_score.csv')  # Turns the tech_self_score csv into a data frame
    df_main = pd.read_csv('main_talent.csv') # Turns the main file into a dataframe
    assert df_tech['id'].max() == len(df_main['id'])  # Checks the max id is equal to the number of ids in the main file



test_get_fields('main')
test_get_fields('self_score')
test_get_fields('strengths')
test_get_fields('weaknesses')
