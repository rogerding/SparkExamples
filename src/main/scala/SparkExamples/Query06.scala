package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rding on 7/30/14.
 * (6) list employee's name and salary whose salary is higher than average salary of whole company

 */
object Query06 {

  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: Query06 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query06")
    val sc = new SparkContext(conf)

    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-06
    val rdd6_1 = sc.textFile(file_1)
    val rdd6_2 = rdd6_1.map(_.split(","))
    // val rdd6_2 = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt").map(_.split(","))

    val salRdd = rdd6_2.map(line => line(5).trim.toInt).cache()
    val avgSal = salRdd.sum / salRdd.count    // 2077.0833333333335


    val rdd6_3 = rdd6_2.map(line => (line(1).trim, line(5).trim.toInt, avgSal))

    //val rdd6_3 = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt").map(_.split(",")).map(line => (line(1).trim, line(5).trim.toInt, avgSal))

    /*
    rdd6_3 = Array[(String, Int, Double)] = Array(
      (SMITH,800,2077.0833333333335), (ALLEN,1600,2077.0833333333335), (WARD,1250,2077.0833333333335),
      (JONES,2975,2077.0833333333335), (MARTIN,1250,2077.0833333333335), (BLAKE,2850,2077.0833333333335),
      (CLARK,2450,2077.0833333333335), (KING,5000,2077.0833333333335), (TURNER,1500,2077.0833333333335),
      (JAMES,950,2077.0833333333335), (FORD,3000,2077.0833333333335), (MILLER,1300,2077.0833333333335)
    )
     */

    val rdd6_4 = rdd6_3.filter((a:(String, Int, Double)) => a._2 > a._3).map(a=>(a._1, a._2))

    /*
    rdd6_4 = Array[(String, Int)] = Array(
      (JONES,2975), (BLAKE,2850), (CLARK,2450),
      (KING,5000), (FORD,3000))

     */


    // better
    val result = rdd6_2.map(line => (line(1).trim, line(5).trim.toInt)).filter(_._2 > avgSal)

    result.saveAsTextFile(args(0))

    sc.stop()
  }
}
