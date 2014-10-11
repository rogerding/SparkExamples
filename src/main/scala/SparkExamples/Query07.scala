package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rding on 7/30/14.
 * (7) list employee's name and dept name whose name start with "J"

 */
object Query07 {
  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  def main(args: Array[String]) {
    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-07
    if (args.length != 1) {
      System.err.println("Usage: Query07 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query07")
    val sc = new SparkContext(conf)



    val rdd7_1 = sc.textFile(file_1).map(_.split(","))
    val rdd7_2 = sc.textFile(file_2).map(_.split(","))

    val rdd7_3 = rdd7_1.map(line => (line(7).trim.toInt, line(1).trim))
    val rdd7_4 = rdd7_2.map(line => (line(0).trim.toInt, line(1).trim))

    /*
    rdd7_3 = Array[(Int, String)] = Array(
      (20,SMITH), (30,ALLEN), (30,WARD), (20,JONES), (30,MARTIN), (30,BLAKE),
      (10,CLARK), (10,KING), (30,TURNER), (30,JAMES), (20,FORD), (10,MILLER)
    )

    rdd7_4 = Array[(Int, String)] = Array((10,ACCOUNTING), (20,RESEARCH), (30,SALES), (40,OPERATIONS))
     */

    val result = rdd7_3.filter(_._2.startsWith("J")).join(rdd7_4).values

    result.saveAsTextFile(args(0))

    sc.stop()
  }
}
