package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rding on 7/30/14.
 * (8) list 3 employee's name and salary with highest salary

 */
object Query08 {
  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition: Int = 1

  def main(args: Array[String]) {
    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-08
    if (args.length != 1) {
      System.err.println("Usage: Query08 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query08")
    val sc = new SparkContext(conf)



    val rdd8_1 = sc.textFile(file_1).map(_.split(","))

    val rdd8_2 = rdd8_1.map(line => (line(5).trim.toInt, line(1).trim)).sortByKey(ascending = false)

    /*
    rdd8_2 = Array[(Int, String)] = Array(
      (5000,KING), (3000,FORD), (2975,JONES), (2850,BLAKE), (2450,CLARK), (1600,ALLEN),
      (1500,TURNER), (1300,MILLER), (1250,WARD), (1250,MARTIN), (950,JAMES), (800,SMITH)
    )
     */

    val result = rdd8_2.map(a => (a._2, a._1)).take(3)


    sc.parallelize(result, num_partition).saveAsTextFile(args(0))

    sc.stop()
  }
}
