package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
//import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rding on 10/10/15.
 * (10) If an person can only communicates with his direct manager, or people directly managed by him, or people in the
 same dept, find the number of inter-person required for communicates between any 2 person
 */
public class Query10 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";

    static int num_partition = 1;

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query10 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query10");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaRDD<String[]> rdd10_1 = sc.textFile(file_1).map(
            new Function<String, String[]>() {
                public String[] call(String line) throws Exception {
                    String[] parts = line.split(",");
                    return parts;
                }
            }
        );


    /*
        val rdd10_2 = rdd10_1.map(line => (line(0).trim, line(3).trim))
    rdd10_2 : Array[(String, String)] = Array(
      (7369,7902), (7499,7698), (7521,7698), (7566,7839), (7654,7698), (7698,7839),
      (7782,7839), (7839,""), (7844,7698), (7900,7698), (7902,7566), (7934,7782)
    )


        // generate all employee id
        //rdd10_3 = rdd10_1.map(line => (1, line(0).trim)).values
    rdd10_3: Array[String] = Array(7369, 7499, 7521, 7566, 7654, 7698, 7782, 7839, 7844, 7900, 7902, 7934)

     */
        JavaPairRDD<String, String> rdd10_2 = rdd10_1.mapToPair(
            new PairFunction<String[], String, String>() {
                @Override
                public Tuple2<String, String> call(String[] array) throws Exception {
                    return new Tuple2<String, String>(array[0], array[3]);
                }
            }
        );

        final Map<String, String> mgrMap = rdd10_2.collectAsMap();;

        JavaPairRDD<Integer, String> rdd10_3_0 = rdd10_1.mapToPair(
            new PairFunction<String[], Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(String[] array) throws Exception {
                    return new Tuple2<Integer, String>(1, array[0]);
                }
            }
        );
        JavaRDD<String> rdd10_3 = rdd10_3_0.values();

/*
(7369,[7369, 7902, 7566, 7839])
(7499,[7499, 7698, 7839])
(7521,[7521, 7698, 7839])
(7566,[7566, 7839])
(7654,[7654, 7698, 7839])
(7698,[7698, 7839])
(7782,[7782, 7839])
(7839,[7839])
(7844,[7844, 7698, 7839])
(7900,[7900, 7698, 7839])
(7902,[7902, 7566, 7839])
(7934,[7934, 7782, 7839])

 */

        JavaPairRDD<String, List<String>> rdd10_4 = rdd10_3.mapToPair(
            new PairFunction<String, String, List<String>>() {
                @Override
                public Tuple2<String, List<String>> call(String s) throws Exception {
                    List<String> mgrList = new ArrayList<String>();
                    mgrList.add(s); // include self
                    String a1 = s;
                    String a2 = null;   // a2 is a1's manager
                    do {
                        a2 = mgrMap.get(a1);
                        if (!a2.isEmpty()) {
                            mgrList.add(a2);
                        }
                        a1 = a2;
                    } while (!a1.isEmpty());
                    return new Tuple2<String, List<String>>(s, mgrList);
                }
            }
        );


        final Map<String, List<String>> mgrChainMap = rdd10_4.collectAsMap();;


        List<Tuple2<String, String>> personPair = new ArrayList<Tuple2<String, String>>();

        List<String>    persons = rdd10_3.collect(); // List<String>
        int personCnt = persons.size();

        for (int i = 0 ; i < personCnt - 1; i++) {
            for (int j = i + 1; j < personCnt; j++) {
                personPair.add(new Tuple2<String, String>(persons.get(i), persons.get(j)));
            }
        }

        JavaRDD<Tuple2<String, String>> rdd10_5 = sc.parallelize(personPair, num_partition);


    /*
        def generatePersonPair(a: Array[String]): Array[(String, String)] = {

            val result = ArrayBuffer[(String, String)]()

            for (i <- 0 until a.length) {
                for (j <- i+1 to a.length - 1) {
                    result.append((a(i), a(j)))
                }
            }
            result.toArray
        }

      val rdd10_5 = sc.parallelize(generatePersonPair(rdd10_3.collect()), num_partition)

      rdd10_5 =  Array[(String, String)] = Array(
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


        JavaRDD<Tuple3<String, String, Integer>> result = rdd10_5.map(
            new Function<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
                private int calculateNodeDistance(int i, String id, List<String> chain_2) {
                    int distance = 0;
                    for (int j = 0; j < chain_2.size(); j++) {
                        if (chain_2.get(j).equals(id)) {
                            distance = i + j - 1;
                            break;
                        }
                    }
                    return distance;
                }

                @Override
                public Tuple3<String, String, Integer> call(Tuple2<String, String> a) throws Exception {
                    if ( mgrMap.get(a._1()).equals(a._2()) || mgrMap.get(a._2()).equals(a._1()) || mgrMap.get(a._1()).equals(mgrMap.get(a._2())) ) {
                        return new Tuple3<String, String, Integer>(a._1(), a._2(), 0);
                    }
                    else {
                        List<String>    mgrChain_1 = mgrChainMap.get(a._1());
                        List<String>    mgrChain_2 = mgrChainMap.get(a._2());
                        int result = -1;

                        for (int i = 0; i < mgrChain_1.size(); i++) {
                            if (mgrChain_2.contains(mgrChain_1.get(i))) {
                                result = calculateNodeDistance(i, mgrChain_1.get(i), mgrChain_2);
                                break;
                            }
                        }
                        return new Tuple3<String, String, Integer>(a._1(), a._2(), result);
                    }
                }
            }
        );

        System.out.println(result.toDebugString());
        result.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query10 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query10 hdfs://quickstart.cloudera:8020/user/cloudera/class/q10
