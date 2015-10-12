SparkExamples
==========

There are 2 tables:

CREATE DATABASE IF NOT EXISTS emp;

CREATE TABLE IF NOT EXISTS emp(EMPNO INT, ENAME STRING, JOB STRING, MGR INT, HIREDATE STRING, SAL INT, COMM INT, DEPTNO INT) 
  ROW FORMAT
  DELIMITED FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dept(DEPTNO INT, DNAME STRING, LOC STRING) 
  ROW FORMAT
  DELIMITED FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE;

LOAD DATA local INPATH 'emp.txt' INTO TABLE emp;
LOAD DATA local INPATH 'dept.txt' INTO TABLE dept;

CREATE TABLE IF NOT EXISTS emp_parquet LIKE emp STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS dept_parquet LIKE dept STORED AS PARQUET;

insert overwrite table emp_parquet select * from emp;
insert overwrite table dept_parquet select * from dept;

SQL> select * from emp;
     
    EMPNO ENAME       JOB        MGR   HIREDATE     SAL    COMM   DEPTNO
---------- ---------- --------- ------ ---------- ------ ----- ---------

    7369, SMITH,      CLERK,     7902, 1980-12-17,   800,     0,  20
    7499, ALLEN,      SALESMAN,  7698, 1981-02-20,  1600,   300,  30
    7521, WARD,       SALESMAN,  7698, 1981-02-22,  1250,   500,  30
    7566, JONES,      MANAGER,   7839, 1981-04-02,  2975,     0,  20
    7654, MARTIN,     SALESMAN,  7698, 1981-09-28,  1250,  1400,  30
    7698, BLAKE,      MANAGER,   7839, 1981-05-01,  2850,     0,  30
    7782, CLARK,      MANAGER,   7839, 1981-06-09,  2450,     0,  10
    7839, KING,       PRESIDENT, NULL, 1981-11-17,  5000,     0,  10
    7844, TURNER,     SALESMAN,  7698, 1981-09-08,  1500,     0,  30
    7900, JAMES,      CLERK,     7698, 1981-12-03,   950,     0,  30
    7902, FORD,       ANALYST,   7566, 1981-12-03,  3000,     0,  20
    7934, MILLER,     CLERK,     7782, 1982-01-23,  1300,     0,  10

    
    
SQL> select * from dept;

    DEPTNO DNAME          LOC
---------- -------------- -------------
        10 ACCOUNTING     NEW YORK
        20 RESEARCH       DALLAS
        30 SALES          CHICAGO
        40 OPERATIONS     BOSTON
        

# Running Environment: #
Cloudera QuickStart VM for CDH 5.4.x

[http://www.cloudera.com/content/cloudera/en/downloads/quickstart_vms/cdh-5-4-x.html](http://www.cloudera.com/content/cloudera/en/downloads/quickstart_vms/cdh-5-4-x.html "Cloudera QuickStart VM for CDH 5.4.x")

# Exercise: #

## (Part 1) ##

Import the data into HDFS (2 files).
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt


## (Part 2) ##
### Write Spark program to: ###

(1) list total salary for each dept.

	select deptno, sum(sal) from emp group by deptno;
	OR	
	select dname, sum(sal) from emp join dept on emp.deptno=dept.deptno group by dname;


(2) list total number of employee and average salary for each dept.

	select deptno, count(empno), avg(sal) from emp group by deptno;
	OR
	select dname, count(empno), avg(sal) from emp join dept on emp.deptno=dept.deptno group by dname;


(3) list the first hired employee's name for each dept.

	select e.deptno, e.ename, e.hiredate from (select deptno, min(cast(hiredate as date)) as mindate from emp group by deptno) a join emp e on e.hiredate=a.mindate AND e.deptno=a.deptno;

	select e.deptno, e.ename, e.hiredate from (select deptno, min(hiredate) as mindate from emp group by deptno) a, emp e where e.hiredate=a.mindate AND e.deptno=a.deptno;


(4) list total employee salary for each city.

	select  loc, sum(sal) from emp join dept on emp.deptno=dept.deptno group by loc;


(5) list employee's name and salary whose salary is higher than their manager

	SELECT e.ename, e.sal FROM emp e JOIN emp e2 ON e.mgr = e2.empno WHERE e.sal > e2.sal;


(6) list employee's name and salary whose salary is higher than average salary of whole company
	
	select b.ename, b.sal from (select avg(sal) AS avgsal from emp) a CROSS JOIN emp b where b.sal > a.avgsal order by b.sal;


(7) list employee's name and dept name whose name start with "J"

	select emp.ename, dept.dname from emp JOIN dept on emp.deptno=dept.deptno and emp.ename like 'J%';

	select ename, dname from emp, dept where emp.deptno=dept.deptno and ename like 'J%';


(8) list 3 employee's name and salary with highest salary

	select ename, sal from emp order by sal desc limit 3;


(9) sort employee by total income (salary+comm), list name and total income.

	select ename, sal+comm as income from emp order by income;


(10) If an person can only communicates with his direct manager, or people directly managed by him, or people in the
     same dept, find the number of inter-person required for communicates between any 2 person
    

### Run Spark program ### (Run as user hdfs, otherwise you will get permission error)
01 	
	
    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-01

02 

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query02 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-02

03 
	
    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query03 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-03

04

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query04 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-04

05 

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query05 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-05

06 

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query06 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-06

07

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query07 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-07

08

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query08 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-08

09

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query09 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-09

10

    spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query10 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-10
