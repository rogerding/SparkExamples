package SparkExamples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer

import scala.util.control._

/**
 * Created by rding on 7/30/14.
 * (10) If an person can only communicates with his direct manager, or people directly managed by him, or people in the
     same dept, find the number of inter-person required for communicates between any 2 person
 */
object Query10 {
  val file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt"
  val file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt"

  val num_partition = 1

  def generatePersonPair(a: Array[String]): Array[(String, String)] = {

    val result = ArrayBuffer[(String, String)]()

    for (i <- 0 until a.length) {
      for (j <- i+1 to a.length - 1) {
        result.append((a(i), a(j)))
      }
    }
    result.toArray
  }

  def buildMgrChain(mgrMap: scala.collection.Map[String,String])(a: String): (String, Array[(String)]) = {

    def buildChain(cur: String, curArray: Array[String]) : Array[String] = {
      if (mgrMap(cur).nonEmpty) buildChain(mgrMap(cur), curArray :+ cur)
      else curArray :+ cur
    }

    (a, buildChain(a, Array[String]()))
  }



  def calculateInterPerson2(mgrMap: Map[String,String], mgrChainMap: Map[String,Array[String]])(a: (String, String)): (String, String, Int) = {

    if ( mgrMap(a._1) == a._2 || mgrMap(a._2) == a._1 || mgrMap(a._1) == mgrMap(a._2) )
        (a._1, a._2, 0)
    else {
        val mgrChain_1 = mgrChainMap(a._1)
        val mgrChain_2 = mgrChainMap(a._2)

        // create a Breaks object
        val loop1 = new Breaks
        val loop2 = new Breaks

        var curMgr_1 = ""
        var curMgr_2 = ""
        var found = false
        var ii = 0
        var jj = 0

        loop1.breakable {
            for (i <- 0 until mgrChain_1.length) {
                curMgr_1 = mgrChain_1(i)
                loop2.breakable {
                    for (j <- 0 until mgrChain_2.length) {
                        curMgr_2 = mgrChain_2(j)
                        if (curMgr_2 == curMgr_1) {
                          found = true
                          jj = j
                          loop2.break()
                        }
                    }
                }

                if (found) {
                    ii = i
                    loop1.break()
                }
            }
        }

        (a._1, a._2, ii + jj - 1)
    }
  }


  def calculateInterPerson(mgrMap: Map[String,String], mgrChainMap: Map[String,Array[String]])(a: (String, String)): (String, String, Int) = {

      def calculateNodeDistance(i: Int, id: String, mgrChain: Array[String]) : Int = {
          val loop = new Breaks
          var distance = 0

          loop.breakable {
            for (j <- 0 until mgrChain.length) {
              if (mgrChain(j) == id) {
                distance = i + j - 1
                loop.break()
              }
            }
          }
          distance
      }

      if ( mgrMap(a._1) == a._2 || mgrMap(a._2) == a._1 || mgrMap(a._1) == mgrMap(a._2) )
          (a._1, a._2, 0)
      else {
          val mgrChain_1 = mgrChainMap(a._1)
          val mgrChain_2 = mgrChainMap(a._2)
          val mgrChain_2_set = mgrChain_2.toSet

          // create a Breaks object
          val loop = new Breaks
          var result = 0

          loop.breakable {
              for (i <- 0 until mgrChain_1.length) {
                  if (mgrChain_2_set.contains(mgrChain_1(i))) {
                      result = calculateNodeDistance(i, mgrChain_1(i), mgrChain_2)
                      loop.break()
                  }
              }
          }

          (a._1, a._2, result)
      }
  }


  def main(args: Array[String]) {
    // args(0) = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/result/out-10
    if (args.length != 1) {
      System.err.println("Usage: Query10 <out_file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Query10")
    val sc = new SparkContext(conf)



    val rdd10_1 = sc.textFile(file_1).map(_.split(","))
    // val rdd10_1 = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt").map(_.split(","))

    val rdd10_2 = rdd10_1.map(line => (line(0).trim, line(3).trim))
    /*
    rdd10_2 : Array[(String, String)] = Array(
      (7369,7902), (7499,7698), (7521,7698), (7566,7839), (7654,7698), (7698,7839),
      (7782,7839), (7839,""), (7844,7698), (7900,7698), (7902,7566), (7934,7782)
    )
     */

    val managerMap = rdd10_2.collectAsMap()
    /*
    managerMap: scala.collection.Map[String,String] = Map(
      7900 -> 7698, 7782 -> 7839, 7698 -> 7839, 7566 -> 7839, 7902 -> 7566, 7839 -> "",
      7521 -> 7698, 7499 -> 7698, 7934 -> 7782, 7844 -> 7698, 7369 -> 7902, 7654 -> 7698)
     */

    // generate all employee id
    val rdd10_3 = rdd10_1.map(line => (1, line(0).trim)).values
    /*
    rdd10_3: Array[String] = Array(7369, 7499, 7521, 7566, 7654, 7698, 7782, 7839, 7844, 7900, 7902, 7934)
     */

    val rdd10_4 = rdd10_3.map(buildMgrChain(managerMap))
    /*
    rdd10_4 : Array[(String, Array[String])] = Array(
      (7369,Array(7369, 7902, 7566, 7839)), (7499,Array(7499, 7698, 7839)),
      (7521,Array(7521, 7698, 7839)), (7566,Array(7566, 7839)),
      (7654,Array(7654, 7698, 7839)), (7698,Array(7698, 7839)),
      (7782,Array(7782, 7839)), (7839,Array(7839)),
      (7844,Array(7844, 7698, 7839)), (7900,Array(7900, 7698, 7839)),
      (7902,Array(7902, 7566, 7839)), (7934,Array(7934, 7782, 7839))
    )
     */

    val rdd10_5 = sc.parallelize(generatePersonPair(rdd10_3.collect()), num_partition)

    /* rdd10_5 =  Array[(String, String)] = Array(
        (7369,7499), (7369,7521), (7369,7566), (7369,7654), (7369,7698), (7369,7782),
        (7369,7839), (7369,7844), (7369,7900), (7369,7902), (7369,7934),

        (7499,7521), (7499,7566), (7499,7654), (7499,7698), (7499,7782), (7499,7839),
        (7499,7844), (7499,7900), (7499,7902), (7499,7934),

        (7521,7566), (7521,7654), (7521,7698), (7521,7782), (7521,7839), (7521,7844),
        (7521,7900), (7521,7902), (7521,7934),

        (7566,7654), (7566,7698), (7566,7782), (7566,7839), (7566,7844), (7566,7900),
        (7566,7902), (7566,7934),

        (7654,7698), (7654,7782), (7654,7839), (7654,7844), (7654,7900), (7654,7902),
        (7654,7934),

        (7698,7782), (7698,7839), (7698,7844), (7698,7900), (7698,7902), (7698,7934),

        (7782,7839), (7782,7844), (7782,7900), (7782,7902), (7782,7934),

        (7839,7844), (7839,7900), (783...

     */

    // need to convert scala.collection.Map to scala.collection.immutable.Map
    val result = rdd10_5.map(calculateInterPerson(Map(managerMap.toSeq: _*), Map(rdd10_4.collectAsMap().toSeq: _*)))



    /*
     result: Array[(String, String, Int)] = Array(
      (7369,7499,4), (7369,7521,4), (7369,7566,1), (7369,7654,4), (7369,7698,3), (7369,7782,3),
      (7369,7839,2), (7369,7844,4), (7369,7900,4), (7369,7902,0), (7369,7934,4),

      (7499,7521,0), (7499,7566,2), (7499,7654,0), (7499,7698,0), (7499,7782,2), (7499,7839,1),
      (7499,7844,0), (7499,7900,0), (7499,7902,3), (7499,7934,3),

      (7521,7566,2), (7521,7654,0), (7521,7698,0), (7521,7782,2), (7521,7839,1), (7521,7844,0),
      (7521,7900,0), (7521,7902,3), (7521,7934,3),

      (7566,7654,2), (7566,7698,0), (7566,7782,0), (7566,7839,0), (7566,7844,2), (7566,7900,2),
      (7566,7902,0), (7566,7934,2),

      (7654,7698,0), (7654,7782,2), (7654,7839,1), (7654,7844,0), (7654,7900,0), (7654,7902,3), (7654,7934,3),

      (7698,7782,0), (7698,7839,0), (7698,7844,0), (7698,7900,0), (7698,7902,2), (769...

     */

    result.saveAsTextFile(args(0))

    sc.stop()
  }
}

