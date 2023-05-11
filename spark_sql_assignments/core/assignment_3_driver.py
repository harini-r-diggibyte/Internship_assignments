from spark_sql_assignments.core.assignment_3_utils import *

#creating a sparkSession
spark=sparkSession()

#creating the dataFrame
df_new=dataFrame_creation(spark)

#getting the first row each department wise
firstRow=first_row_dept_wise(df_new)

#each department wise printing the hightest salary
high_sal=highest_sal_dept_wise(df_new)

#calculating highest salary, lowest salary, average Salary, finding total salary for each department
aggregation_values=high_low_avg_totSal(df_new)