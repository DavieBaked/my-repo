import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Task_1 import Advanced_SQL as asql
import duckdb
import pytest
import numpy as np
import subprocess



" This is a subprocess to reload the loan.db database "
load_script_path = 'Task_1/database/database_load.py'
subprocess.run(['python3', load_script_path], check=True)

cursor = duckdb.connect("Task_1/database/loan.db")

qry1 = asql.question_1()
qry2 = asql.question_2()
qry3 = asql.question_3()
qry4 = asql.question_4()
qry5 = asql.question_5()
qry6 = asql.question_6()
qry7 = asql.question_7()




@pytest.fixture
def df1():
    " This fixture returns a DataFrame created from the execution of the question_1 SQL query 'qry1'. "
    return cursor.execute(qry1).df()

def test_question_1_shape(df1):
    " Test to ensure the shape of the DataFrame 'df1' is correct "
    assert df1.shape == (3, 2)

def test_question_1_val_A(df1):
    " Test to ensure that the total count of the 'A' CustomerClass in 'df1' is correct "
    filtr = df1['CustomerClass'] == 'A'
    assert df1[filtr].iloc[0,1].round() == 64263

def test_question_1_val_Ap(df1):
    " Test to ensure that the total count of the 'A+' CustomerClass in 'df1' is correct "
    filtr = df1['CustomerClass'] == 'A+'
    assert df1[filtr].iloc[0,1].round() == 76921

def test_question_1_value_B(df1):
    " Test to ensure that the total count of the 'B' CustomerClass in 'df1' is correct "
    filtr = df1['CustomerClass'] == 'B'
    assert df1[filtr].iloc[0,1].round() == 64805

def test_question_1_re():
    " Test to ensure that the JOIN function was used " 
    assert "join" in qry1.lower()




@pytest.fixture
def df2():
    " This fixture returns a DataFrame created from the execution of the question_2 SQL query 'qry2'. "
    return cursor.execute(qry2).df()

def test_question_2_shape(df2):
    " Test to ensure the shape of the DataFrame 'df2' is correct "
    assert df2.shape == (9,2)

def test_question_2_sum(df2):
    " Test to ensure that the sum of the 'RejectedApplications' column in 'df2' is correct "
    assert df2['RejectedApplications'].sum() == 533

def test_question_2_EC(df2):
    " Test to ensure that the count of Eastern Cape 'RejectedApplications' in 'df2' is correct "
    filtr = (df2['Province'] == 'EasternCape') | (df2.iloc[:,0] == 'EC')
    assert df2[filtr]['RejectedApplications'].iloc[0] == 68

def test_question_2_NL(df2):
    " Test to ensure that the count of Natal 'RejectedApplications' in 'df2' is correct "
    filtr = (df2['Province'] == 'Natal') | (df2.iloc[:,0] == 'NL')
    assert df2[filtr]['RejectedApplications'].iloc[0] == 51

def test_question_2_NW(df2):
    " Test to ensure that the count of NorthWest 'RejectedApplications' in 'df2' is correct "
    filtr = (df2['Province'] == 'NorthWest') | (df2.iloc[:,0] == 'NW')
    assert df2[filtr]['RejectedApplications'].iloc[0] == 50

def test_question_2_re():
    " Test to ensure that the JOIN function was used " 
    assert "join" in qry2.lower()






#QUESTION 3 TESTS:
    

def test_question_3_query():
    " Test to run the query multiple times to reinsert data if the query isn't structured correctly "
    cursor.execute(qry3)
    cursor.execute(qry3)
    cursor.execute(qry3)
    pass

@pytest.fixture
def df3():
    " This fixture returns a DataFrame of the financing table created in Question_3. "
    qry = """SELECT * FROM financing"""
    return cursor.execute(qry).df()


def test_question_3_shape(df3):
    " Test to ensure the shape of the DataFrame 'df3' is correct "
    assert df3.shape == (1000,7)


def test_question_3_columns(df3):
    " Test to ensure the correct columns were created in the DataFrame 'df3' "
    assert df3.columns.to_list() == ['CustomerID','Income','LoanAmount','LoanTerm','InterestRate','ApprovalStatus','CreditScore']


def test_question_3_re():
    " Test to ensure that the INSERT function was used " 
    assert "insert" in qry3.lower()

    



def test_question_4_query():
    cursor.execute(qry4)
    cursor.execute(qry4)
    cursor.execute(qry4)
    pass


@pytest.fixture
def df4():  
    " This fixture returns a DataFrame of the timeline table created in Question_4. "
    qry = """SELECT * FROM timeline"""
    return cursor.execute(qry).df()


def test_question_4_shape(df4):
    " Test to ensure the shape of the DataFrame 'df4' is correct "
    assert df4.shape == (12000,4)


def test_question_4_columns(df4):
    " Test to ensure the correct columns were created in the DataFrame 'df4' "
    assert df4.columns.to_list() == ['CustomerID', 'MonthName', 'NumberofRepayments', 'AmountTotal']


def test_question_4_sum(df4):
    " Test to ensure that the sum of the 'AmountTotal' column in 'df4' is correct "
    assert df4['AmountTotal'].sum() == 619744


def test_question_4_value(df4):
    " Test to check if the correct value is found in the DataFrame 'df4' "
    filtr = (df4['CustomerID'] == 856) & (df4['MonthName'] == 'September')
    assert df4[filtr]['AmountTotal'].iloc[0] == 351


def test_question_4_re():
    " Test to ensure that the CROSS JOIN function was used " 
    assert "cross join" in qry4.lower()


def test_question_4_null(df4):
    " Test to ensure that NULL value's were replaced with 0's " 
    assert df4['AmountTotal'].iloc[0] == 0





@pytest.fixture
def df5(): 
    " This fixture returns a DataFrame created from the execution of the question_5 SQL query 'qry5'. " 
    return cursor.execute(qry5).df()



def test_question_5_shape(df5):
    " Test to ensure the shape of the DataFrame 'df5' is correct "
    assert df5.shape == (1000,25)



def test_question_5_columns(df5):
    " Test to ensure the correct columns were created in the DataFrame 'df5' "
    assert df5.columns.to_list() == ['CustomerID','JanuaryRepayments','JanuaryTotal','FebruaryRepayments','FebruaryTotal','MarchRepayments','MarchTotal','AprilRepayments','AprilTotal','MayRepayments','MayTotal','JuneRepayments','JuneTotal','JulyRepayments','JulyTotal','AugustRepayments','AugustTotal','SeptemberRepayments','SeptemberTotal','OctoberRepayments','OctoberTotal','NovemberRepayments','NovemberTotal','DecemberRepayments','DecemberTotal']



def test_question_5_sum(df5):
    " Test to ensure that the sum of the 'JanuaryTotal' column in 'df5' is correct "
    assert df5['JanuaryTotal'].sum() == 50816


def test_question_5_type(df5):
    " Test to ensure the Repayment columns were set to Integer type "
    assert (df5['FebruaryRepayments'].dtype == np.int32) | (df5['FebruaryRepayments'].dtype == np.int64)






@pytest.fixture
def df6():  
    " This fixture returns a DataFrame created from the execution of the question_6 SQL query 'qry6'. "
    return cursor.execute(qry6).df()


def test_question_6_shape(df6):
    " Test to ensure the shape of the DataFrame 'df6' is correct "
    assert df6.shape == (1000,4)
    

def test_question_6_age_1(df6):
    " Test to ensure the Null value CorrectedAges were correctly replaced "
    assert df6[df6["CustomerID"] == 1]["CorrectedAge"].iloc[0]  == 52


def test_question_6_age_2(df6):
    " Test to ensure the Null value CorrectedAges were correctly replaced "
    assert df6[df6["CustomerID"] == 2]["CorrectedAge"].iloc[0]  == 71


def test_question_6_age_7(df6):
    " Test to ensure the Null value CorrectedAges were correctly replaced "
    assert df6[df6["CustomerID"] == 7]["CorrectedAge"].iloc[0]  == 39


def test_question_6_age_8(df6):
    " Test to ensure the Null value CorrectedAges were correctly replaced "
    assert df6[df6["CustomerID"] == 8]["CorrectedAge"].iloc[0]  == 51


def test_question_6_age_3(df6):
    " Test to ensure the CorrectedAges values were correctly shifted "
    assert df6[df6["CustomerID"] == 3]["CorrectedAge"].iloc[0]  == 24


def test_question_6_columns(df6):
    " Test to ensure the correct columns were created in the DataFrame 'df6' "
    assert df6.columns.to_list() == ['CustomerID', 'Age', 'CorrectedAge', 'Gender']


def test_question_6_re(df6):
    " Test to ensure that the LAG and OVER functions were used " 
    assert "lag" in qry6.lower() and "over" in qry6.lower()




@pytest.fixture
def df7():  
    " This fixture returns a DataFrame created from the execution of the question_7 SQL query 'qry7'. "
    return cursor.execute(qry7).df()


def test_question_7_shape(df7):
    " Test to ensure the shape of the DataFrame 'df7' is correct "
    assert df7.shape == (1000,7)


def test_question_7_re(df7):
    " Test to ensure that the DENSE_RANK and OVER functions were used " 
    assert "dense_rank" in qry7.lower() and "over" in qry7.lower()



def test_question_7_count_adult(df7):
    " Test to ensure the count for 'Adult' in the 'AgeCategory' is correct "
    assert df7.value_counts('AgeCategory')['Adult'] == 471



def test_question_7_count_Teen(df7):
    " Test to ensure the count for 'Teenager' in the 'AgeCategory' is correct "
    assert df7.value_counts('AgeCategory')['Teenager'] == 36



def test_question_7_max_total(df7):
    " Test to ensure the max value in the 'RepaymentTotal' column is correct "
    assert max(df7['RepaymentTotal']) == 13



def test_question_7_max_rank(df7):
    " Test to ensure the max value in the 'Rank' column is correct "
    assert max(df7['Rank']) == 13




" This is a subprocess to reload the loan.db database "
load_script_path = 'Task_1/database/database_load.py'
subprocess.run(['python3', load_script_path], check=True)