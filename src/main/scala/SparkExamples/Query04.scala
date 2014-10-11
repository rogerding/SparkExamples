package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rding on 7/26/14.
 */

// (4) list total employee salary for each city.


object Query04 {
  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"
  val num_partition = 1


  def main(args: Array[String]) {
    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-04
    if (args.length != 1) {
      System.err.println("Usage: Query04 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query04")
    val sc = new SparkContext(conf)



    val rdd4_1 = sc.textFile(file_1).map(_.split(","))
    val rdd4_2 = sc.textFile(file_2).map(_.split(","))

    val rdd4_3 = rdd4_1.map(line => (line(7).trim.toInt, line(5).trim.toInt))
    val rdd4_4 = rdd4_2.map(line => (line(0).trim.toInt, line(2).trim))
    /*
    rdd4_3 = Array[(Int, Int)] = Array(
      (20,800), (30,1600), (30,1250), (20,2975),
      (30,1250), (30,2850), (10,2450), (10,5000),
      (30,1500), (30,950), (20,3000), (10,1300)
    )

    rdd4_4 = Array[(Int, String)] = Array(
      (10,NEW YORK), (20,DALLAS), (30,CHICAGO), (40,BOSTON)
    )

     */
    val rdd4_5 = rdd4_3.join(rdd4_4)
    /*
    rdd4_5 = Array[(Int, (Int, String))] = Array(
      (30,(1600,CHICAGO)), (30,(1250,CHICAGO)), (30,(1250,CHICAGO)), (30,(2850,CHICAGO)), (30,(1500,CHICAGO)), (30,(950,CHICAGO)),
      (20,(800,DALLAS)), (20,(2975,DALLAS)), (20,(3000,DALLAS)),
      (10,(2450,NEW YORK)), (10,(5000,NEW YORK)), (10,(1300,NEW YORK))
    )
     */

    val rdd4_6 = rdd4_5.map(a => a._2)
    /*
    rdd4_6 = Array[(Int, String)] = Array(
      (1600,CHICAGO), (1250,CHICAGO), (1250,CHICAGO), (2850,CHICAGO), (1500,CHICAGO), (950,CHICAGO),
      (800,DALLAS), (2975,DALLAS), (3000,DALLAS),
      (2450,NEW YORK), (5000,NEW YORK), (1300,NEW YORK)
    )
    */

    val rdd4_7 = rdd4_6.map(a => (a._2, a._1))
    /*
    rdd4_7: Array[(String, Int)] = Array(
      (CHICAGO,1600), (CHICAGO,1250), (CHICAGO,1250), (CHICAGO,2850), (CHICAGO,1500), (CHICAGO,950),
      (DALLAS,800), (DALLAS,2975), (DALLAS,3000),
      (NEW YORK,2450), (NEW YORK,5000), (NEW YORK,1300)
    )
    */

    val rdd4_8 = rdd4_7.reduceByKey(_+_)
    /*
    rdd4_8: Array[(String, Int)] = Array((CHICAGO,9400), (NEW YORK,8750), (DALLAS,6775))
    */


    rdd4_8.saveAsTextFile(args(0))

    sc.stop()
  }
}
