from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from spark_sql_assignments.core.assignment_2_utils import *

#creating a spark session
spark=spark_creation()

#creating a dataFrame
df=create_dataFrame(spark)

#creating pivot table
pivot_table=func_pivot(df)

#creating an unpivot table
unpivot_table=func_unpivot(pivot_table)