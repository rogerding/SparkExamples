package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/*
EMPNO ENAME      JOB        MGR   HIREDATE   SAL    COMM   DEPTNO
---------- ---------- --------- ------ ---------- ------ ----- ---------

7369, SMITH,      CLERK,     7902, 17-12-80,   800,     0,  20
7499, ALLEN,      SALESMAN,  7698, 20-02-81,  1600,   300,  30
7521, WARD,       SALESMAN,  7698, 22-02-81,  1250,   500,  30
7566, JONES,      MANAGER,   7839, 02-04-81,  2975,     0,  20
7654, MARTIN,     SALESMAN,  7698, 28-09-81,  1250,  1400,  30
7698, BLAKE,      MANAGER,   7839, 01-05-81,  2850,     0,  30
7782, CLARK,      MANAGER,   7839, 09-06-81,  2450,     0,  10
7839, KING,       PRESIDENT,     , 17-11-81,  5000,     0,  10
7844, TURNER,     SALESMAN,  7698, 08-09-81,  1500,     0,  30
7900, JAMES,      CLERK,     7698, 03-12-81,   950,     0,  30
7902, FORD,       ANALYST,   7566, 03-12-81,  3000,     0,  20
7934, MILLER,     CLERK,     7782, 23-01-82,  1300,     0,  10
*/


/**
 * Created by rding on 7/26/14.
 */

// (3) list the first hired employee's name for each dept.


object Query03 {

  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  type StringPair = (String, String)

  def findEarlier(a: StringPair, b: StringPair): StringPair = {
    val hireDate_a = a._2.split("-")
    val year_a = hireDate_a(0).toInt
    val month_a = hireDate_a(1).toInt
    val day_a = hireDate_a(2).toInt

    val hireDate_b = b._2.split("-")
    val year_b = hireDate_b(0).toInt
    val month_b = hireDate_b(1).toInt
    val day_b = hireDate_b(2).toInt

    if ( (year_a * 365 + month_a * 30 + day_a) <= (year_b * 365 + month_b * 30 + day_b) )
      a
    else
      b
  }


  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: Query03 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query03")
    val sc = new SparkContext(conf)


    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-03

    val rdd1 = sc.textFile(file_1, num_partition)
    val rdd2 = rdd1.map(_.split(","))

    val rdd3 = rdd2.map(line => (line(7).trim.toInt, (line(1).trim, line(4).trim)))

    /*
    rdd3 = Array[(Int, (String, String))] = Array(
      (20,(SMITH,1980-12-17)), (30,(ALLEN,1981-02-20)), (30,(WARD,1981-02-22)),
      (20,(JONES,1981-04-02)), (30,(MARTIN,1981-09-28)), (30,(BLAKE,1981-05-01)),
      (10,(CLARK,1981-06-09)), (10,(KING,1981-11-17)), (30,(TURNER,1981-09-08)),
      (30,(JAMES,1981-12-03)), (20,(FORD,1981-12-03)), (10,(MILLER,1982-01-23))
     )

     */

    val rdd4 = rdd3.reduceByKey(findEarlier)
    /*
    rdd4 = Array[(Int, (String, String))] = Array(
        (30,(ALLEN,1981-02-20)),
        (20,(SMITH,1980-12-17)),
        (10,(CLARK,1981-06-09))
    )
     */

    rdd4.saveAsTextFile(args(0))

    sc.stop()
  }
}
