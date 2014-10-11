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
    7839, KING,       PRESIDENT,     , 1981-11-17,  5000,     0,  10
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

Exercise:
Import the data into hadoop
Write Spark program to:
(1) list total salary for each dept.
(2) list total number of employee and average salary for each dept.
(3) list the first hired employee's name for each dept.
(4) list total employee salary for each city.
(5) list employee's name and salary whose salary is higher than their manager
(6) list employee's name and salary whose salary is higher than average salary of whole company
(7) list employee's name and dept name whose name start with "J"
(8) list 3 highest salary's employee name and salary
(9) sort employee by total income (salary+comm), list name and total income.
(10) If an person can only communicates with his direct manager, or people directly managed by him, or people in the
     same dept, find the number of inter-person required for communicates between any 2 person


====================================================================
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt

output:
/home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar


$ spark-submit --master spark://quickstart.cloudera:7077
               --class SparkExamples.Query01
               --executor-memory 1g
               /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar
               hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-01

$ hdfs dfs -rm -r -f /user/cloudera/data/emp/spark-result/

spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-01
spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query02 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-02
spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query03 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-03
spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query04 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-04
spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query05 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-05

spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query10 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-10

// combine output(part-00000, part-00001) to one file

$ hdfs dfs -getmerge hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/spark-result/out-04 result04.txt

$ spark-shell --master spark://quickstart.cloudera:7077 --executor-memory 2g