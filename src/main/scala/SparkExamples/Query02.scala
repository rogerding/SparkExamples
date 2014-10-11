package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rding on 7/26/14.
 */

// list total number of employee and average salary for each dept.
object Query02 {

  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: Query02 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query02")
    val sc = new SparkContext(conf)


    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-02

    val rdd1 = sc.textFile(file_1, num_partition)
    val rdd2 = rdd1.map(_.split(","))

    val rdd3 = rdd2.map(line => (line(7).trim.toInt, line(5).trim.toInt))
    //val rdd3 = rdd2.map(line => Pair(line(7).trim.toInt, line(5).trim.toInt))

    //val rdd3 = sc.textFile(file_1).map(_.split(",")).map(line => (line(7).trim.toInt, line(5).trim.toInt))

    /*
    rdd3 = Array[(Int, Int)] = Array(
      (20,800), (30,1600), (30,1250), (20,2975),
      (30,1250), (30,2850), (10,2450), (10,5000),
      (30,1500), (30,950), (20,3000), (10,1300))
     */

    val rdd4 = rdd3.groupByKey()
    /*
    rdd4 = Array[(Int, Iterable[Int])] = Array(
              (30,ArrayBuffer(1600, 1250, 1250, 2850, 1500, 950)),
              (20,ArrayBuffer(800, 2975, 3000)),
              (10,ArrayBuffer(2450, 5000, 1300))
            )
     */
    // val rdd5 = rdd4.mapValues[U](f:Iterable[Int]) => U     RDD[(Int, U)]
    val rdd5 = rdd4.mapValues(salary => (salary.size, 1.0 * salary.sum / salary.size))
    /*
    rdd5 = Array[(Int, (Int, Double))] = Array(
      (30,(6,1566.6666666666667)),
      (20,(3,2258.3333333333335)),
      (10,(3,2916.6666666666665))
    )
    */
    rdd5.saveAsTextFile(args(0))


    ///////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////
    // another way, use combineByKey
    val rdd6 = rdd3.combineByKey(
        (v:Int) => (1, v),
        (acc: (Int, Int), v) => (acc._1 + 1, acc._2 + v),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    /*
    rdd6: Array[(Int, (Int, Int))] = Array((30,(6,9400)), (20,(3,6775)), (10,(3,8750)))

     */

    val rdd7 = rdd6.map{ case (key, value) => (key, value._2 / value._1.toFloat) }

    rdd7.collectAsMap().map(println(_))

    /*
    rdd.reduceByKey(func)   produces the same RDD as
    rdd.groupByKey().mapValues(value => value.reduce(func))
    but is more efficient as it avoids the step of creating a list of values for each key.
     **/

    //val rdd3_3 = rdd3.reduceByKey((v1:Int, v2:Int) => (1+1, v1+v2))

    sc.stop()
  }
}
