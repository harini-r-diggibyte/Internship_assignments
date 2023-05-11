# Internship_assignments #
This repository contains all my assignment codes under Python, Spark, Pyspark, Spark SQL. 

###  Python ###
The directory "python_assignments" contains all the python assignments.

###  Spark ###
The directory "spark_assignments" contains all the Spark Assignments.

#### Spark Assignment 1 ####
Given two data files transaction.csv, user.csv which is placed under resource directory.

#### user file consists of the following fields: ####
  - user_Id
  - EmailId
  - NativeLanguage
  - Location
#### transaction file consists of the following fields: ####
  - Transaction_id
  - product_id
  - userId
  - price
  - product_description
  
userId in both the files denotes the same.

#### Tasks to be performed with above files are as follows: ####
  - 1.Count of unique locations where each product is sold.
  - 2.Find out products bought by each user.
  - 3.Total spending done by each user on each product.
  
#### Code snippet consists of functions as below : ####

  - user_DataFrame : 
  This function reads the user.csv file and data is been stored into a dataframe.
  - transaction_DataFrame : 
  This function reds the transaction.csv file and data is been stored into another dataframe.
  - join_DataFrame : 
  This function joins the two dataframes based on inner join condition with userid column.
  - unique_loc_count : 
  This functions accepts the joined result of two dataframes as an argument and computes the count of unique locations where each product is sold.
  - user_prod : 
  This function accepts the joined result of two dataframes as an argument and computes the products bought by each user.
  -tot_spend : 
  This function accepst the joined result of two dataframes as an argument and computes the total amount of spending done by each user on each product.
  
core directory has two files
  - driver : We call a function from driver and the result expected from that function is seen in the driver 
  - util   : body of the fucntion is described in util
Whenever we call a function from driver, the function is been executed in util and result is been displayed in driver.

resource directory has two files transaction.csv, user.csv in it.

test directory has the test case in it.


#### Spark Assignment 2 ####

Each log line comprises of a standard part (up to .rb:) and an operation-specific part. The standard part fields are like so: 
  - Logging level, one of DEBUG, INFO, WARN, ERROR (separated by ,) 
  - A timestamp (separated by ,) 
  - The downloader id, denoting the downloader instance (separated by --) 
  
The retrieval stage, denoted by the Ruby class name, one of: 
  - event_processing 
  - ght_data_retrieval 
  - api_client 
  - retriever 
  - ghtorrent 
  
#### Tasks to be performed with above files are as follows: ####
  - 1.Write a function to load it in an Data Frame. 
  - 2.How many lines does the Data Frame contain?	 
  - 3.Count the number of WARNing messages 
  - 4.How many repositories where processed in total? Use the api_client lines only. 
  - 5.Which client did most HTTP requests? 
  - 6.Which client did most FAILED HTTP requests? Use group_by to provide an answer. 
  - 7.What is the most active hour of day? 
  - 8.What is the most active repository (hint: use messages from the ghtorrent.rb layer only)? 

#### Code snippet consists of functions as below : ####

  - create_torrent_DataFrame()
This function separates the fields(log_level,timeStamp,ghTorrent_details) in textFile with the given delimiter ","

  - ghtorrent_fields()
This function accepts the dataFrame created by create_torent_DatFrame as a parameter.
This function extracts the various fields as per the requirements

  - total_line()
This function accepts the dataFrame created by ghtorrent_fields as a parameter.
This function result will give the count of the total number of lines in that dataFrame which has the text file in it.

  - warn_count()
This function accepts the dataFrame created by ghtorrent_fields as a parameter
This function result will give the count of the total number of lines in textFile which has loglevel as "WARN"

  - api_client_repo()
This function accepts the dataFrame created by ghtorrent_fields as a parameter
This function result will give the count on how many repositories where processed under "api_client"

  - most_http()
This function accepts the dataFrame created by ghtorrent_fields as a parameter  
This function gives the client who gave most http request

  - failed_request()
This function accepts the dataFrame created by ghtorrent_fields as a parameter  
This function gives the client who had most failed http request

  - active_hour()
This function accepts the dataFrame created by create_torrent_DataFrame as a parameter   
This function gives the most active hour of the day

  - active_repo()
This function accepts the dataFrame created by ghtorrent_fields as a parameter  
This function gives the most active repository

core directory has two files
  - driver : We call a function from driver and the result expected from that function is seen in the driver 
  - util   : body of the fucntion is described in util
Whenever we call a function from driver, the function is been executed in util and result is been displayed in driver.

resource directory has ghtorrent-logs text file in it.

test directory has the test case in it.

###  PySpark ###
The directory "pyspark_assignments" contains the pyspark assignments.

Create a table in the below structure.
Product Name	   Issue Date	    Price	  Brand	    Country	Product number
Washing Machine	 1648770933000	20000	  Samsung	  India	  0001
Refrigerator	   1648770999000	35000	  LG	      null	  0002
Air Cooler	     1648770948000	45000	  Voltas	  null	  0003

It is referred as table 1.

This is done by the function create_table()

After completing the creation, we work on it to satisfy the below scenarios.

  - Convert the Issue Date with the timestamp format.

Example: 
Input: 1648770933000 -> Output: 2022-03-31T23:55:33.000+0000

This is done by the function timestamp_to_unixTime()

  - Convert timestamp to date type	  

Example: 
Input: 2022-03-31T23:55:33.000+0000 -> Output: 2022-03-31

This is done by the function convert_date()

  - Remove the starting extra space in Brand column for LG and Voltas fields

This is done by the function trim_spaces()

  - Replace null values with empty values in Country column 

This is done by the function replace_null_with_empty_values()


Create another table with the below data and referred as table 2.

SourceId	TransactionNumber	Language	ModelNumber	StartTime	                      Product Number
150711	  123456	          EN	      456789	    2021-12-27T08:20:29.842+0000    0001
150439	  234567	          UK	      345678	    2021-12-27T08:21:14.645+0000    0002
150647	  345678	          ES	      234567	    2021-12-27T08:22:42.445+0000    0003

This is done by the function creating_table_2()

After creating table, we work on it to satisfy the below scenarios.

  - Change the camel case columns to snake case 
  
Example: 
SourceId: source_id
TransactionNumber: transaction_number

This is done by the function column_case_conversion()

  - Add another column as start_time_ms and convert the values of StartTime to milliseconds.

Example: 
Input: 2021-12-27T08:20:29.842+0000 -> Output: 1640593229842
Input: 2021-12-27T08:21:14.645+0000 -> Output: 1640593274645

This is done by the function timestamp_to_unix_timestamp()

  - Combine both the tables based on the Product Number 
        - and get all the fields in return.
        - And get the country as EN
joining of tables is done by the function join_table()

Filtering the records based on the language column value "EN" is done by the function filter_records()

# SQL-Assignments #

This repository contains SQL Assignments done using Spark and SSMS.

## SQL Assignments using Spark ##

assignment_1_driver and assignment_1_utils contains Spark SQL Assignment 1.
assignment_2_driver and assignment_2_utils contains Spark SQL Assignment 2.
assignment_3_driver and assignment_3_utils contains Spark SQL Assignment 3.


### SQL Assignment 1  (SQL_1) ###

Create a table with data in the below structure:
{"firstname":"James","middlename":"","lastname":"Smith"},"03011998","M",3000

  - Column1 : name - {"firstname":"James","middlename":"","lastname":"Smith"}
  - Column2 : dob - "03011998"
  - Column3 : gender - "M"
  -Column4 : Salary - 3000
  
Perform the below tasks in the created table :
  
  
  - Select firstName,lastName and salary from dataFrame.
  
  - Add Country, department, and age column in the dataframe. 

  - Change the value of salary column. 

  - Change the data types of DOB and salary to String  

  - Derive new column from salary column. 

  - Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition) 

  - Filter the name column whose salary in maximum.  

  - Drop the department and age column. 

  - List out distinct value of dob and salary 
  
  
### SQL Assignment 2 (SQL_2) ###

Create a non-nested dataframe with product, amount and country fields. 

        ("Banana",1000,"USA")
        ("Carrots",1500,"India")
        ("Beans",1600,"Sweden")
        ("Orange",2000,"UK")
        ("Orange",2000,"UAE")
        ("Banana",400,"China")
        ("Carrots",1200,"China")
        
Perform the below tasks on the created table :

  -  Find total amount exported to each country of each product. 

  -  Perform unpivot function on output of above question. 
  
  
### SQL Assignment 3 (SQL_3) ###

Create a data frame with employee_name, department and salary. 

Perform below tasks on the created dataFrame

  - Select first row from each department group. 

  - Create a Dataframe from Row and List of tuples. 

  - Apply Schema while creating a Dataframe. 

  - Retrieve Employees who earns the highest salary. 

  - Select the highest, lowest, average, and total salary for each department group.  


## SQL Assignment using SSMS ##

sql_Assignments_SSMS directory contains SQL Assignments done using SSMS setup.
