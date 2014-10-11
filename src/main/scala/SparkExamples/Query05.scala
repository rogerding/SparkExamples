package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
// import scala.Array


/**
 * Created by rding on 7/26/14.
 * (5) list employee's name and salary whose salary is higher than their manager
 */

object Query05 {
  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"
  val num_partition = 1

  type MyType = (Int, String, Int)

  def findEmployee(myArray: Iterable[MyType]): Array[(String, Int)] = {
    var managerSalary = 0
    for (a <- myArray) {
      if (a._1 == 0) {
        managerSalary = a._3
      }
    }

    val result = ArrayBuffer[(String, Int)]()
    for (a <- myArray) {
      if ( (a._1 == 1) && (a._3 > managerSalary) ) {
        result.append((a._2, a._3))
      }
    }

    result.toArray
  }

  def flatArray(element: (Int, Array[(String, Int)])): Array[(Int, String, Int)] = {
    val result = ArrayBuffer[(Int, String, Int)]()
    for (a <- element._2) {
      result.append((element._1, a._1, a._2))
    }

    result.toArray
  }

  def main(args: Array[String]) {
    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-05
    if (args.length != 1) {
      System.err.println("Usage: Query05 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query05")
    val sc = new SparkContext(conf)



    val rdd5_1 = sc.textFile(file_1).map(_.split(","))

    // (myself id, (0, name, salary))
    val rdd5_2 = rdd5_1.map(line => (line(0).trim.toInt, (0, line(1).trim, line(5).trim.toInt)))

    // (my manager id, (1, name, salary))
    val rdd5_3 = rdd5_1.filter(_(3).trim.length>0).map(line => (line(3).trim.toInt, (1, line(1).trim, line(5).trim.toInt)))

    /*
    rdd5_2 =Array[(Int, (Int, String, Int))] = Array(
      (7369,(0,SMITH,800)), (7499,(0,ALLEN,1600)), (7521,(0,WARD,1250)), (7566,(0,JONES,2975)),
      (7654,(0,MARTIN,1250)), (7698,(0,BLAKE,2850)), (7782,(0,CLARK,2450)), (7839,(0,KING,5000)),
      (7844,(0,TURNER,1500)), (7900,(0,JAMES,950)), (7902,(0,FORD,3000)), (7934,(0,MILLER,1300))
    )
    rdd5_3 = Array[(Int, (Int, String, Int))] = Array(
      (7902,(1,SMITH,800)), (7698,(1,ALLEN,1600)), (7698,(1,WARD,1250)), (7839,(1,JONES,2975)),
      (7698,(1,MARTIN,1250)), (7839,(1,BLAKE,2850)), (7839,(1,CLARK,2450)), (7698,(1,TURNER,1500)),
      (7698,(1,JAMES,950)), (7566,(1,FORD,3000)), (7782,(1,MILLER,1300))
    )
     */

    val rdd5_4 = rdd5_2.union(rdd5_3).sortByKey(ascending = true)
    /*
    rdd5_4 =  Array[(Int, (Int, String, Int))] = Array(
      (7369,(0,SMITH,800)),
      (7499,(0,ALLEN,1600)),
      (7521,(0,WARD,1250)),
      (7566,(0,JONES,2975)), (7566,(1,FORD,3000)),
      (7654,(0,MARTIN,1250)),
      (7698,(0,BLAKE,2850)), (7698,(1,ALLEN,1600)), (7698,(1,WARD,1250)), (7698,(1,MARTIN,1250)), (7698,(1,TURNER,1500)), (7698,(1,JAMES,950)),
      (7782,(0,CLARK,2450)), (7782,(1,MILLER,1300)),
      (7839,(0,KING,5000)), (7839,(1,JONES,2975)), (7839,(1,BLAKE,2850)), (7839,(1,CLARK,2450)),
      (7844,(0,TURNER,1500)),
      (7900,(0,JAMES,950)),
      (7902,(0,FORD,3000)), (7902,(1,SMITH,800)),
      (7934,(0,MILLER,1300))
    )
     */

    val rdd5_5 = rdd5_4.groupByKey(1) // one partition
    /*
    rdd5_5 = Array[(Int, Iterable[(Int, String, Int)])] = Array(
      (7566,ArrayBuffer((0,JONES,2975), (1,FORD,3000))),
      (7369,ArrayBuffer((0,SMITH,800))),
      (7654,ArrayBuffer((0,MARTIN,1250))),
      (7521,ArrayBuffer((0,WARD,1250))),
      (7499,ArrayBuffer((0,ALLEN,1600))),
      (7698,ArrayBuffer((0,BLAKE,2850), (1,ALLEN,1600), (1,WARD,1250), (1,MARTIN,1250), (1,TURNER,1500), (1,JAMES,950))),
      (7782,ArrayBuffer((0,CLARK,2450), (1,MILLER,1300))),
      (7839,ArrayBuffer((0,KING,5000), (1,JONES,2975), (1,BLAKE,2850), (1,CLARK,2450))),
      (7934,ArrayBuffer((0,MILLER,1300))),
      (7844,ArrayBuffer((0,TURNER,1500))),
      (7900,ArrayBuffer((0,JAMES,950))),
      (7902,ArrayBuffer((0,FORD,3000), (1,SMITH,800)))
    )
     */

    val rdd5_6 = rdd5_5.mapValues(findEmployee)
    /*
    rdd5_6 = Array[(Int, scala.collection.mutable.ArrayBuffer[(String, Int)])] = Array(
      (7566,ArrayBuffer((FORD,3000))),
      (7369,ArrayBuffer()),
      (7654,ArrayBuffer()),
      (7521,ArrayBuffer()),
      (7499,ArrayBuffer()),
      (7698,ArrayBuffer()),
      (7782,ArrayBuffer()),
      (7839,ArrayBuffer()),
      (7934,ArrayBuffer()),
      (7844,ArrayBuffer()),
      (7900,ArrayBuffer()),
      (7902,ArrayBuffer())
    )

     */

    val rdd5_7 = rdd5_6.filter(_._2.nonEmpty)
                              // _._2.nonEmpty, _._2.size > 0, _._2.length > 0

    //rdd5_7.saveAsTextFile(args(0))

    /*
    rdd5_7 = Array[(Int, Array[(String, Int)])] = Array(
      (7566,Array((FORD,3000)))
    )
     */



    val rdd5_8 = rdd5_7.map(flatArray)
    // rdd5_8: Array[Array[(Int, String, Int)]] = Array(Array((7566,FORD,3000)))

    val rdd5_9 = rdd5_8.flatMap(a=>a)
    // rdd5_9: Array[(Int, String, Int)] = Array((7566,FORD,3000))

    rdd5_9.saveAsTextFile(args(0))


  /*
    val result = ArrayBuffer[Int]()

    result.append(1)
    result.toArray.toList.flatten
   */
    sc.stop()
  }
}
