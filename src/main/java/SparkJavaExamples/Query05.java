package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by rding on 10/10/15.
 * list employee's name and salary whose salary is higher than their manager
 * SELECT e.ename, e.sal FROM emp e JOIN emp e2 ON e.mgr = e2.empno WHERE e.sal > e2.sal;
 */
public class Query05 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";


    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query05 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query05");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaRDD<String[]> rdd5_1 = sc.textFile(file_1).map(
                new Function<String, String[]>() {
                    public String[] call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return parts;
                    }
                }
        );

        // (myself id, (0, name, salary))
        // val rdd5_2 = rdd5_1.map(line => (line(0).trim.toInt, (0, line(1).trim, line(5).trim.toInt)))
        JavaPairRDD<Integer, Tuple3<Integer, String, Integer>> rdd5_2 = rdd5_1.mapToPair(
            new PairFunction<String[], Integer, Tuple3<Integer, String, Integer>>() {
                @Override
                public Tuple2<Integer, Tuple3<Integer, String, Integer>> call(String[] array) throws Exception {
                    return new Tuple2<>(new Integer(
                            array[0].trim()),
                            new Tuple3<>(0, array[1].trim(), new Integer(array[5].trim()))
                    );
                }
            }
        );

        // (my manager id, (1, name, salary))
        // val rdd5_3 = rdd5_1.filter(_(3).trim.length>0).map(line => (line(3).trim.toInt, (1, line(1).trim, line(5).trim.toInt)))

        JavaPairRDD<Integer, Tuple3<Integer, String, Integer>> rdd5_3 = rdd5_1.filter(
            new Function<String[], Boolean>() {
                @Override
                public Boolean call(String[] array) throws Exception {
                    return !(array[3].trim().isEmpty());
                }
            }
        ).mapToPair(
            new PairFunction<String[], Integer, Tuple3<Integer, String, Integer>>() {
                public Tuple2<Integer, Tuple3<Integer, String, Integer>> call(String[] array) throws Exception {
                    return new Tuple2<>(new Integer(
                            array[3].trim()),
                            new Tuple3<>(1, array[1].trim(), new Integer(array[5].trim()))
                    );
                }
            }
        );


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
         /*
        //val rdd5_4 = rdd5_2.union(rdd5_3).sortByKey(ascending = true)
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

        JavaPairRDD<Integer, Tuple3<Integer, String, Integer>> rdd5_4 = rdd5_2.union(rdd5_3).sortByKey(true);


/*

        val rdd5_5 = rdd5_4.groupByKey(1) // one partition
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

        val rdd5_6 = rdd5_5.mapValues(findEmployee)
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
     rdd5_7 = Array[(Int, Array[(String, Int)])] = Array(
      (7566,Array((FORD,3000)))
    )

     */

        JavaPairRDD<Integer, Iterable<Tuple3<Integer, String, Integer>>> rdd5_5 = rdd5_4.groupByKey(1);

        JavaPairRDD<Integer, Iterable<Tuple2<String, Integer>>> rdd5_6 = rdd5_5.mapValues(
            new Function<Iterable<Tuple3<Integer, String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
                @Override
                public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple3<Integer, String, Integer>> tuple3s) throws Exception {
                    int managerSalary = 0;

                    for (Tuple3<Integer, String, Integer> a : tuple3s) {
                        if (0 == a._1()) {
                            managerSalary = a._3();
                        }
                    }

                    List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();

                    for (Tuple3<Integer, String, Integer> a : tuple3s) {
                        if ( (1 == a._1()) && ((a._3() > managerSalary)) ) {
                            // add the employee info
                            result.add(new Tuple2<String, Integer>(a._2(), a._3()));
                        }
                    }
                    return result;
                }
            }
        );

        //     val rdd5_7 = rdd5_6.filter(_._2.nonEmpty)
        JavaPairRDD<Integer, Iterable<Tuple2<String, Integer>>> rdd5_7 = rdd5_6.filter(
            new Function<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Integer, Iterable<Tuple2<String, Integer>>> a) throws Exception {
                    Iterable<Tuple2<String, Integer>> it = a._2();
                    Iterator<Tuple2<String, Integer>> ita = it.iterator();
                    return ita.hasNext();
                }
            }
        );

        /////////////////////////////////////////////////////////////////////////

        // rdd5_7 = (7566,[(FORD,3000)])

        //val rdd5_8 = rdd5_7.map(flatArray)
        // rdd5_8: Array[Array[(Int, String, Int)]] = Array(Array((7566,FORD,3000)))

        //val rdd5_9 = rdd5_8.flatMap(a=>a)
        // rdd5_9: Array[(Int, String, Int)] = Array((7566,FORD,3000))

        JavaRDD<Iterable<Tuple3<Integer, String, Integer>>> rdd5_8 = rdd5_7.map(
            new Function<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>, Iterable<Tuple3<Integer, String, Integer>>>() {
                @Override
                public Iterable<Tuple3<Integer, String, Integer>> call(Tuple2<Integer, Iterable<Tuple2<String, Integer>>> a) throws Exception {
                    List<Tuple3<Integer, String, Integer>> result = new ArrayList<Tuple3<Integer, String, Integer>>();

                    for (Tuple2<String, Integer> b : a._2()) {
                        result.add(new Tuple3<>(a._1(), b._1(), b._2()));
                    }

                    return result;
                }
            }
        );

        // rdd5_8 = [(7566,FORD,3000)]

        JavaRDD<Tuple3<Integer, String, Integer>> rdd5_9 = rdd5_8.flatMap(
                new FlatMapFunction<Iterable<Tuple3<Integer, String, Integer>>, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public Iterable<Tuple3<Integer, String, Integer>> call(Iterable<Tuple3<Integer, String, Integer>> a) throws Exception {
                        return a;
                    }
                }
        );
        // rdd5_9 = (7566,FORD,3000)


        System.out.println(rdd5_9.toDebugString());
        rdd5_9.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query05 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query05 hdfs://quickstart.cloudera:8020/user/cloudera/class/q5
