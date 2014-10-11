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

// (1) list total salary for each dept.

/*
spark-submit --master spark://quickstart.cloudera:7077 --class SparkExamples.Query01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8000/user/cloudera/data/emp/result/out-01
 */

object Query01 {

  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  /*
  sc.textFile(file_1, num_partition)
    .map(_.split(","))
    .map(line => (line(7).trim.toInt, line(5).trim.toInt))
    .reduceByKey(_+_)
    .saveAsTextFile(args(0))
   */

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: Query01 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query01")  //.setMaster("spark://quickstart.cloudera:7077")
    val sc = new SparkContext(conf)


    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-01


    val rdd1 = sc.textFile(file_1, num_partition)
    val rdd2 = rdd1.map(_.split(","))
    /*
    rdd2 = Array[Array[String]] = Array(
        Array(7369, " SMITH", "      CLERK", "     7902", " 17-12-80", "   800", "     0", "  20"),
        Array(7499, " ALLEN", "      SALESMAN", "  7698", " 20-02-81", "  1600", "   300", "  30"),
        Array(7521, " WARD", "       SALESMAN", "  7698", " 22-02-81", "  1250", "   500", "  30"),
        Array(7566, " JONES", "      MANAGER", "   7839", " 02-04-81", "  2975", "     0", "  20"),
        Array(7654, " MARTIN", "     SALESMAN", "  7698", " 28-09-81", "  1250", "  1400", "  30"),
        Array(7698, " BLAKE", "      MANAGER", "   7839", " 01-05-81", "  2850", "     0", "  30"),
        Array(7782, " CLARK", "      MANAGER", "   7839", " 09-06-81", "  2450", "     0", "  10"),
        Array(7839, " KING", "       PRESIDENT", "     ", " 17-11-81", "  5000", "     0", "  10"),
        Array(7844, " TURNER", "  ...
        )
     */

    val rdd3 = rdd2.map(line => (line(7).trim.toInt, line(5).trim.toInt))
    /*
    rdd3 = Array[(Int, Int)] = Array(
      (20,800), (30,1600), (30,1250), (20,2975),
      (30,1250), (30,2850), (10,2450), (10,5000),
      (30,1500), (30,950), (20,3000), (10,1300))
     */


    val rdd4 = rdd3.reduceByKey(_+_)
    /*
    rdd4 = Array[(Int, Int)] = Array((30,9400), (20,6775), (10,8750))
     */

    rdd4.toDebugString
    rdd4.saveAsTextFile(args(0))

    sc.stop()
  }
}


/*
spark-submit --master spark://quickstart.cloudera:7077
/home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar
--class SparkExamples.Query01  --executor-memory 256m
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt 2


spark-submit --master spark://quickstart.cloudera:7077 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar --class com.cloudera.sparkwordcount.SparkWordCount  --executor-memory 256m hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt 2
*/