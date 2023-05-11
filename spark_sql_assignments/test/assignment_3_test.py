from  spark_sql_assignments.core.assignment_3_utils import *
import unittest

class TestMyFunc(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=SparkSession.builder.getOrCreate()
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

#testing dataframe
    def testdataFrame(self):
        #creation of dataFrame
        data = [("James", "Sales", 3000),
                ("Michael", "Sales", 4600),
                ("Robert", "Sales", 4100),
                ("Maria", "Finance", 3000),
                ("Raman", "Finance", 3000),
                ("Scott", "Finance", 3300),
                ("Jen", "Finance", 3900),
                ("Jeff", "Marketing", 3000),
                ("Kumar", "Marketing", 2000)]
        schema = StructType([StructField("EmpName", StringType(), True),
                             StructField("Department", StringType(), True),
                             StructField("Salary", IntegerType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)

        #to find the first row in each department (by grouping department column)
        data=[("Maria","Finance",3000),
              ("Kumar","Marketing",2000),
              ("James","Sales",3000)]
        schema=StructType([StructField("EmpName",StringType(),True),
                           StructField("Department", StringType(), True),
                           StructField("Salary", IntegerType(), True)
                           ])
        expected_df=self.spark.createDataFrame(data=data,schema=schema)
        transformed_df=first_row_dept_wise(df)
        self.assertEqual(transformed_df.collect(),expected_df.collect())

        # testing of retrieval highest salaried person in each group
        data = [("Jen", "Finance", 3900),
                ("Jeff", "Marketing", 3000),
                ("Michael", "Sales", 4600)]
        schema = StructType([StructField("EmpName", StringType(), True),
                             StructField("Department", StringType(), True),
                             StructField("Salary", IntegerType(), True)
                             ])
        expected_df = self.spark.createDataFrame(data=data, schema=schema)
        transformed_df=highest_sal_dept_wise(df)
        self.assertEqual(transformed_df.collect(), expected_df.collect())

        #testing of obtaining highest, lowest, average and total_salary under each department.
        data = [("Finance",3300.0,3900,3000,13200),
                ("Marketing",2500.0,3000,2000,5000),
                ("Sales",3900.0,4600,3000,11700)]
        schema = StructType([StructField("Department", StringType(), True),
                             StructField("Average", FloatType(), True),
                             StructField("highest_salary", IntegerType(), True),
                             StructField("lowest_salary", IntegerType(), True),
                             StructField("total_salary", IntegerType(), True)
                             ])
        expected_df = self.spark.createDataFrame(data=data, schema=schema)
        transformed_df = high_low_avg_totSal(df)
        self.assertEqual(transformed_df.collect(), expected_df.collect())