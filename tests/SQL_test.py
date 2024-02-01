import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Task_1 import SQL as sql
import duckdb
import pytest
import subprocess




" This is a subprocess to reload the loan.db database "
load_script_path = 'Task_1/database/database_load.py'
subprocess.run(['python3', load_script_path], check=True)




cursor = duckdb.connect("Task_1/database/loan.db")

qry1 = sql.question_1()
qry2 = sql.question_2()
qry3 = sql.question_3()
qry4 = sql.question_4()
qry5 = sql.question_5()




@pytest.fixture
def df1():
    "This fixture returns a DataFrame created from the execution of the question_1 SQL query 'qry1'."
    return cursor.execute(qry1).df()

def test_question_1_shape(df1):
    " Test to ensure the shape of the DataFrame 'df1' is correct "
    assert df1.shape == (27, 2)

def test_question_1_counts(df1):
    " Test to validate that no Name-Surname combination is repeated in the DataFrame 'df1'. "
    assert max(df1[['Name','Surname']].value_counts()) == 1




@pytest.fixture
def df2():
    "This fixture returns a DataFrame created from the execution of the question_2 SQL query 'qry2'."
    return cursor.execute(qry2).df()

def test_question_2_shape(df2):
    " Test to ensure the shape of the DataFrame 'df2' is correct "
    assert df2.shape == (488, 3)

def test_question_2_sum(df2):
    " Test to ensure that the sum of the 'Income' column in 'df2' is correct "
    assert df2['Income'].sum() == 31571942




@pytest.fixture
def df3():
    " This fixture returns a DataFrame created from the execution of the question_3 SQL query 'qry3'. "
    return cursor.execute(qry3).df()

def test_question_3_shape(df3):
    " Test to ensure the shape of the DataFrame 'df3' is correct "
    assert df3.shape == (5, 2)

def test_question_3_value(df3):
    " Test to ensure that the number of Approved loan applications is correct "
    filtr = df3['LoanTerm'] == 48
    assert round(df3[filtr].iloc[0,1]) == 53




@pytest.fixture
def df4():
    " This fixture returns a DataFrame created from the execution of the question_4 SQL query 'qry4'. "
    return cursor.execute(qry4).df()

def test_question_4_shape(df4):
    " Test to ensure the shape of the DataFrame 'df4' is correct "
    assert df4.shape == (3,2)

def test_question_4_A(df4):
    " Test to ensure the number of A class customers is correct "
    filtr = df4['CustomerClass'] == 'A'
    assert df4[filtr]['Count'].iloc[0] == 480

def test_question_4_B(df4):
    " Test to ensure the number of B class customers is correct "
    filtr = df4['CustomerClass'] == 'B'
    assert df4[filtr]['Count'].iloc[0] == 514






def test_execute_question_5():
    " Function to execute question_5 and update credit table "
    cursor.execute(qry5).df()

@pytest.fixture
def df5():
    """This fixture returns a DataFrame created from the execution of the question_4 SQL query 'qry4' on the updated credit table
       from question 5."""
    return cursor.execute(qry4).df()

def test_question_5_shape(df5):
    " Test to ensure the shape of the DataFrame 'df4' is correct "
    assert df5.shape == (4,2)

def test_question_5_A(df5):
    " Test to ensure the number of A class customers is correct "
    filtr = df5['CustomerClass'] == 'A'
    assert df5[filtr]['Count'].iloc[0] == 480

def test_question_5_B(df5):
    " Test to ensure the number of B class customers is correct "
    filtr = df5['CustomerClass'] == 'B'
    assert df5[filtr]['Count'].iloc[0] == 243

def test_question_5_C(df5):
    " Test to ensure the number of C class customers is correct "
    filtr = df5['CustomerClass'] == 'C'
    assert df5[filtr]['Count'].iloc[0] == 271

def test_question_5_reg():
    " Test to ensure that the UPDATE function was used " 
    assert "update" in qry5.lower()











" This is a subprocess to reload the loan.db database "
load_script_path = 'Task_1/database/database_load.py'
subprocess.run(['python3', load_script_path], check=True)