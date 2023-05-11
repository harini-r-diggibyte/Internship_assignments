# SQl Assignment 1

#creating a table called emp
create table emp(firstName varchar(30),middleName varchar(30),lastName varchar(30),dob int, gender char(1),salary int )

#inserting values into the table
insert into emp values('James','','Smith',03011998,'M',3000)
insert into emp values('Michael','Rose','',10011998,'M',20000)
insert into emp values('Robert','','Williams',02012000,'M',3000)
insert into emp values('Maria','Annie','Jones',03011998,'F',11000)
insert into emp values('Jen','Mary','Brown',04101998,'F',10000)

select * from emp

#selecting firstName,lastName,Salary column from the table emp
select firstName,lastName,salary from emp

#adding new column Country,Department,Age to the table emp
alter table emp add Country varchar(20)

alter table emp add Department varchar(20)

alter table emp add age int

#updating the salary column value by incrementing it to 1000
update emp set salary=salary+1000

#creating a new salary column which has salary incremented by a value of 5000
select *,salary+5000 as new_salary from emp

#renaming the firstName, middleName, lastName as firstPosition,secondPosition,thirdPosition respectively.
exec sp_RENAME 'emp.firstName','firstPosition','column'

exec sp_RENAME 'emp.middleName','secondPosition','column'

exec sp_RENAME 'emp.lastName','thirdPosition','column'

#printing the firstName,middleName,lastName of the emp who is getting the highest salary
select firstPosition,secondPosition,thirdPosition from emp where salary in (select max(salary) from emp)

#dropping the columns aege and department from the table emp
alter table emp drop column age
alter table emp drop column department

#selecting distinct values of column dob from emp table
select distinct dob from emp

#selecting distinct values of column salary from emp table
select distinct salary from emp

#changing the datatype of dob and salary column to string
alter table emp alter column dob varchar(10)

alter table emp alter column salary varchar(10)
#to view the data type of columns in a table emp
select * from information_schema.columns where table_name='emp'

#SQL Assignment 2

#creating table fruit_details
create table fruit_details(product varchar(20),amount int, Country varchar(20) )

#inserr=ting values into the fruit_details table
insert into fruit_details values('Banana', 1000, 'USA')

insert into fruit_details values('Carrots', 1500, 'India')

insert into fruit_details values('Beans', 1600, 'Sweden')

insert into fruit_details values('Orange', 2000, 'UK')

insert into fruit_details values('Orange', 2000, 'UAE')

insert into fruit_details values('Banana', 400, 'China')

insert into fruit_details values('Carrots', 1200, 'China')

#pivot columns operation on the table fruit_table
SELECT * from fruit_details
pivot( sum(amount) for Country in (USA,India,Sweden,UK,UAE,China)) as PivotTable

#unPivot columns operation on the table fruit_table
SELECT *
FROM
(
SELECT * FROM fruit_details
PIVOT
(
SUM(amount) FOR Country IN (USA,India,Sweden,UK,UAE,China)
) AS PivotTable
) P
UNPIVOT
(
Price FOR Country IN (USA,India,Sweden,UK,UAE,China)
)
AS UnpivotTable

#SQL Assignment 3

#creating a table employee_details
create table employee_details (emp_name varchar(20),Department varchar(20),salary int)

#inserting values into the table employee_details
insert into employee_details values('James','Sales',3000)

insert into employee_details values('Michael','Sales',4600)

insert into employee_details values('Robert','Sales',4100)

insert into employee_details values('Maria','Finance',3000)

insert into employee_details values('Raman','Finance',3000)

insert into employee_details values('Scott','Finance',3300)

insert into employee_details values('Jen','Finance',3900)

insert into employee_details values('Jeff','Marketting',3000)

insert into employee_details values('Kumar','Marketting',2000)

select * from employee_details

#selecting first row from each department which contains all details of particular employee
SELECT emp_name,department,salary FROM (
  SELECT emp_name, department, salary,
         ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary asc) AS row_num
  FROM employee_details
) subquery
WHERE row_num = 1;

#selecting employee's who get highest salary from each department
SELECT emp_name,department,salary FROM (
  SELECT emp_name, department, salary,
         ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employee_details
) subquery
WHERE row_num = 1;

#Displaying average salary in each department
SELECT department, avg(salary) as avg_salary
FROM employee_details
GROUP BY department;

#Displaying the total of all salaries in each department
SELECT department, sum(salary) as total_salary
FROM employee_details
GROUP BY department;

#Displaying minimum salary in each department
SELECT department, min(salary) as min_salary
FROM employee_details
GROUP BY department;

#Displaying maximum salary in each department
SELECT department, max(salary) as max_salary
FROM employee_details
GROUP BY department;
