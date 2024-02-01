import duckdb


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

    qry = """
    SELECT
        cr.CustomerClass,
        AVG(c.Income) as AverageIncome
    FROM
        customers c
    JOIN
        credit cr ON c.CustomerID = cr.CustomerID
    GROUP BY
        cr.CustomerClass;
    """

    return qry








def question_2():    
    
    #Q2: Make use of a JOIN to return a breakdown of the number of 'RejectedApplications' per 'Province'. 

    qry = """
    SELECT
    CASE
        WHEN Region = 'EC' THEN 'EasternCape'
        WHEN Region = 'GT' THEN 'Gauteng'
        WHEN Region = 'WC' THEN 'WesternCape'
        WHEN Region = 'NW' THEN 'NorthWest'
        WHEN Region = 'NC' THEN 'NorthernCape'
        WHEN Region = 'NL' THEN 'Natal'
        WHEN Region = 'FS' THEN 'FreeState'
        WHEN Region = 'LP' THEN 'Limpopo'
        WHEN Region = 'MP' THEN 'Mpumalanga'
        ELSE c.Region
    END as Province,
    COUNT(*) as RejectedApplications
    FROM
        customers c
    JOIN
        loans l ON c.CustomerID = l.CustomerID
    WHERE
        l.ApprovalStatus = 'Rejected'
    GROUP BY
        Province;
    """ 
    return qry






def question_3():    
    
    # Making use of the `INSERT` function, create a new table called `financing` which will include the following columns:
        # `CustomerID`,`Income`,`LoanAmount`,`LoanTerm`,`InterestRate`,`ApprovalStatus` and `CreditScore`
    # Do not return the new table

    qry = """
    CREATE TABLE IF NOT EXISTS financing (
        CustomerID INT,
        Income DECIMAL(10, 2),
        LoanAmount DECIMAL(10, 2),
        LoanTerm INT,
        InterestRate DECIMAL(5, 2),
        ApprovalStatus VARCHAR(10),
        CreditScore INT
    );

    TRUNCATE TABLE financing;

    INSERT INTO financing (CustomerID, Income, LoanAmount, LoanTerm, InterestRate, ApprovalStatus, CreditScore)
    SELECT 
        c.CustomerID, 
        c.Income, 
        l.LoanAmount, 
        l.LoanTerm, 
        l.InterestRate, 
        l.ApprovalStatus, 
        cr.CreditScore
    FROM 
        (SELECT DISTINCT * FROM customers) c
    JOIN
        (SELECT DISTINCT * FROM loans) l ON c.CustomerID = l.CustomerID
    JOIN
        (SELECT DISTINCT * FROM credit) cr ON c.CustomerID = cr.CustomerID;

    """

    return qry





# Question 4 and 5 are linked

def question_4():

    # Using a `CROSS JOIN` and the `months` table, create a new table called `timeline` that sumarizes Repayments per customer per month.
    # Columns should be: `CustomerID`, `MonthName`, `NumberOfRepayments`, `AmountTotal`.
    # Repayments should only occur between 6am and 6pm London Time.
    # Hint: there should be 12x CustomerID = 1.
    # Null values to be filled with 0.

    qry = """
    SET TIME ZONE 'Europe/London';

    DROP TABLE IF EXISTS timeline;
    CREATE TABLE timeline AS (
        WITH CustomerMonths AS (
            SELECT DISTINCT c.CustomerID, m.MonthID, m.MonthName
            FROM customers c
            CROSS JOIN months m
        )
        SELECT 
            cm.CustomerID, 
            cm.MonthName, 
            COUNT(r.RepaymentID) AS NumberofRepayments, 
            SUM(COALESCE(r.Amount, 0)) AS AmountTotal,
        FROM CustomerMonths cm 
        LEFT JOIN repayments r 
            ON cm.CustomerID = r.CustomerID 
            AND EXTRACT(MONTH FROM r.RepaymentDate) = cm.MonthID
            AND EXTRACT(HOUR FROM r.RepaymentDate AT TIME ZONE r.TimeZone) BETWEEN 6 AND 17
        GROUP BY cm.CustomerID, cm.MonthID, cm.MonthName
        ORDER BY cm.CustomerID, cm.MonthID);
    """
    return qry




def question_5():

    # Make use of conditional aggregation to pivot the `timeline` table such that the columns are as follows:
        # CustomerID, JanuaryRepayments, JanuaryTotal,...,DecemberRepayments, DecemberTotal,...etc
    # MonthRepayments columns (e.g JanuaryRepayments) should be integers
    # Hint: there should be 1x CustomerID = 1


    qry = """ 
    SELECT 
    CustomerID,
    CAST(SUM(CASE WHEN MonthName = 'January' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS JanuaryRepayments,
    SUM(CASE WHEN MonthName = 'January' THEN AmountTotal ELSE 0 END) AS JanuaryTotal,
    CAST(SUM(CASE WHEN MonthName = 'February' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS FebruaryRepayments,
    SUM(CASE WHEN MonthName = 'February' THEN AmountTotal ELSE 0 END) AS FebruaryTotal,
    CAST(SUM(CASE WHEN MonthName = 'March' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS MarchRepayments,
    SUM(CASE WHEN MonthName = 'March' THEN AmountTotal ELSE 0 END) AS MarchTotal,
    CAST(SUM(CASE WHEN MonthName = 'April' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS AprilRepayments,
    SUM(CASE WHEN MonthName = 'April' THEN AmountTotal ELSE 0 END) AS AprilTotal,
    CAST(SUM(CASE WHEN MonthName = 'May' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS MayRepayments,
    SUM(CASE WHEN MonthName = 'May' THEN AmountTotal ELSE 0 END) AS MayTotal,
    CAST(SUM(CASE WHEN MonthName = 'June' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS JuneRepayments,
    SUM(CASE WHEN MonthName = 'June' THEN AmountTotal ELSE 0 END) AS JuneTotal,
    CAST(SUM(CASE WHEN MonthName = 'July' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS JulyRepayments,
    SUM(CASE WHEN MonthName = 'July' THEN AmountTotal ELSE 0 END) AS JulyTotal,
    CAST(SUM(CASE WHEN MonthName = 'August' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS AugustRepayments,
    SUM(CASE WHEN MonthName = 'August' THEN AmountTotal ELSE 0 END) AS AugustTotal,
    CAST(SUM(CASE WHEN MonthName = 'September' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS SeptemberRepayments,
    SUM(CASE WHEN MonthName = 'September' THEN AmountTotal ELSE 0 END) AS SeptemberTotal,
    CAST(SUM(CASE WHEN MonthName = 'October' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS OctoberRepayments,
    SUM(CASE WHEN MonthName = 'October' THEN AmountTotal ELSE 0 END) AS OctoberTotal,
    CAST(SUM(CASE WHEN MonthName = 'November' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS NovemberRepayments,
    SUM(CASE WHEN MonthName = 'November' THEN AmountTotal ELSE 0 END) AS NovemberTotal,
    CAST(SUM(CASE WHEN MonthName = 'December' THEN NumberofRepayments ELSE 0 END) AS INTEGER) AS DecemberRepayments,
    SUM(CASE WHEN MonthName = 'December' THEN AmountTotal ELSE 0 END) AS DecemberTotal
    FROM 
        timeline
    GROUP BY 
        CustomerID;
    """

    return qry





#QUESTION 6 and 7 are linked

def question_6():

    # The `customers` table was created by merging two separate tables: one containing data for male customers and the other for female customers.
    # Due to an error, the data in the age columns were misaligned in both original tables, resulting in a shift of two places upwards in
    # relation to the corresponding CustomerID.

    # Utilize a window function to correct this mistake in a new `CorrectedAge` column.
    # Return the `CustomerID`, `Age`, `CorrectedAge`, `Gender` columns
    # Null values can be input manually

    qry = """ 
    WITH customerlag AS (
        SELECT CustomerID,
               Name,
               Surname,
               Age,
               Gender,
               Income,Region,
            LAG(Age, 2) OVER(PARTITION BY Gender ORDER BY CustomerID) AS CorrectedAge
        FROM (SELECT DISTINCT * FROM customers)
    )
    SELECT
        CustomerID,
        Age,
        CASE 
            WHEN CorrectedAge IS NULL AND CustomerID = 1 THEN 52
            WHEN CorrectedAge IS NULL AND CustomerID = 2 THEN 71
            WHEN CorrectedAge IS NULL AND CustomerID = 7 THEN 39
            WHEN CorrectedAge IS NULL AND CustomerID = 8 THEN 51
            ELSE CorrectedAge 
        END AS CorrectedAge,
        Gender,
    FROM customerlag
    """

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

    qry = """ 
    DROP TABLE IF EXISTS RepaymentCounts;
    
    CREATE TABLE RepaymentCounts AS (
        SELECT
            CustomerID,
            COUNT(RepaymentID) AS RepaymentCount
        FROM Repayments
        GROUP BY CustomerID
    );

    WITH RankedRepayments AS (
        SELECT
            c.CustomerID,
            c.Age,
            c.Income,
            rc.RepaymentCount,
            CASE 
                WHEN c.Age BETWEEN 18 AND 19 THEN 'Teenager'
                WHEN c.Age BETWEEN 20 AND 29 THEN 'Young Adult'
                WHEN c.Age BETWEEN 30 and 60 THEN 'Adult'
                WHEN c.Age > 60 THEN 'Pensioner'
                ELSE 'Adult'
            END AS AgeCategory
        FROM (SELECT DISTINCT * FROM customers) AS c
        LEFT JOIN RepaymentCounts rc ON c.CustomerID = rc.CustomerID
    )
    SELECT
        CustomerID,
        Age,
        Income,
        RepaymentCount,
        COALESCE(RepaymentCount, 0) AS RepaymentTotal,
        AgeCategory,
        DENSE_RANK() OVER(PARTITION BY AgeCategory ORDER BY RepaymentCount DESC) AS Rank
    FROM RankedRepayments 
    """

    return qry
