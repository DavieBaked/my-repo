#hi

"""
The database loan.db consists of 3 tables: 
   1. customers - table containing customer data
   2. loans - table containing loan data pertaining to customers
   3. credit - table containing credit and creditscore data pertaining to customers
   4. repayments - table containing loan repayment data pertaining to customers
   5. months - table containing month name and month ID data
    
You are required to make use of your knowledge in SQL to query the database object (saved as loan.db) and return the requested information.
Simply fill in the vacant space wrapped in triple quotes per question (each function represents a question)

"""


def question_1():    
    
    #Make use of a JOIN to find out the `AverageIncome` per `CustomerClass`

    qry = """___"""

    return qry








def question_2():    
    
    # Make use of a JOIN to return a breakdown of the number of 'RejectedApplications' per 'Province'. 

    qry = """___""" 
    return qry






def question_3():    
    
    # Making use of the `INSERT` function, create a new table called `financing` which will include the following columns:
        # `CustomerID`,`Income`,`LoanAmount`,`LoanTerm`,`InterestRate`,`ApprovalStatus` and `CreditScore`
    # Do not return the new table

    qry = """___"""

    return qry





# Question 4 and 5 are linked

def question_4():

    # Using a `CROSS JOIN` and the `months` table, create a new table called `timeline` that sumarizes Repayments per customer per month.
    # Columns should be: `CustomerID`, `MonthName`, `NumberOfRepayments`, `AmountTotal`.
    # Repayments should only occur between 6am and 6pm London Time.
    # Hint: there should be 12x CustomerID = 1.
    # Null values to be filled with 0.

    qry = """___"""
    return qry




def question_5():

    # Make use of conditional aggregation to pivot the `timeline` table such that the columns are as follows:
        # CustomerID, JanuaryRepayments, JanuaryTotal,...,DecemberRepayments, DecemberTotal,...etc
    # MonthRepayments columns (e.g JanuaryRepayments) should be integers
    # Hint: there should be 1x CustomerID = 1


    qry = """___"""

    return qry





#QUESTION 6 and 7 are linked

def question_6():

    # The `customers` table was created by merging two separate tables: one containing data for male customers and the other for female customers.
    # Due to an error, the data in the age columns were misaligned in both original tables, resulting in a shift of two places upwards in
    # relation to the corresponding CustomerID.

    # Utilize a window function to correct this mistake in a new `CorrectedAge` column.
    # Return the `CustomerID`, `Age`, `CorrectedAge`, `Gender` columns
    # Null values can be input manually

    qry = """___"""

    return qry


def question_7():

    # Create a column that categorizes customers by age called 'AgeCategory'
    # Age categories should be as follows:
        # Teen: x < 20
        # Young Adult: 20 <= x < 30
        # Adult: 30 <= x < 60
        # Pensioner: x >= 60
    # Make use of a windows function to assign a rank to each customer based on the total number of repayments per age group. This rank must
    # appear in the Rank column.
    # Customers with no repayments should be included in the result.

    qry = """___"""

    return qry
