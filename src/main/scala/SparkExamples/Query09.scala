package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rding on 7/30/14.
 * (9) sort employee by total income (salary+comm), list name and total income.

 */
object Query09 {
  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  def combineIncome(a: (Int, String, String)): (Int, String) = {
    if (a._2.size > 0)
      (a._1 + a._2.toInt, a._3)
    else
      (a._1, a._3)
  }

  def main(args: Array[String]) {
    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-09
    if (args.length != 1) {
      System.err.println("Usage: Query09 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query09")
    val sc = new SparkContext(conf)



    val rdd9_1 = sc.textFile(file_1).map(_.split(","))
    // val rdd9_1 = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt").map(_.split(","))

    val rdd9_2 = rdd9_1.map(line => (line(5).trim.toInt, line(6).trim, line(1).trim))

    val rdd9_3 = rdd9_2.map(combineIncome)

    /* rdd9_3 = Array[(Int, String)] = Array(
        (800,SMITH), (1900,ALLEN), (1750,WARD), (2975,JONES), (2650,MARTIN), (2850,BLAKE),
        (2450,CLARK), (5000,KING), (1500,TURNER), (950,JAMES), (3000,FORD), (1300,MILLER)
      )
     */

    val result = rdd9_3.sortByKey(ascending = false).map(a => (a._2, a._1))



    result.saveAsTextFile(args(0))

    sc.stop()
  }
}
