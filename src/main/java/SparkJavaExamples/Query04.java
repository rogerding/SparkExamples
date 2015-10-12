package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by rding on 10/10/15.
 * (4) list total employee salary for each city.
 */
public class Query04 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";


    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query04 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query04");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String[]> rdd4_1 = sc.textFile(file_1).map(
            new Function<String, String[]>() {
                public String[] call(String line) throws Exception {
                    String[] parts = line.split(",");
                    return parts;
                }
            }
        );

        JavaRDD<String[]> rdd4_2 = sc.textFile(file_2).map(
            new Function<String, String[]>() {
                public String[] call(String line) throws Exception {
                    String[] parts = line.split(",");
                    return parts;
                }
            }
        );

  /*
    val rdd4_3 = rdd4_1.map(line => (line(7).trim.toInt, line(5).trim.toInt))
    val rdd4_4 = rdd4_2.map(line => (line(0).trim.toInt, line(2).trim))

    rdd4_3 = Array[(Int, Int)] = Array(
      (20,800), (30,1600), (30,1250), (20,2975),
      (30,1250), (30,2850), (10,2450), (10,5000),
      (30,1500), (30,950), (20,3000), (10,1300)
    )

    rdd4_4 = Array[(Int, String)] = Array(
      (10,NEW YORK), (20,DALLAS), (30,CHICAGO), (40,BOSTON)
    )
  */
        JavaPairRDD<Integer, Integer> rdd4_3 = rdd4_1.mapToPair(
            new PairFunction<String[], Integer, Integer>() {
                @Override
                public Tuple2<Integer, Integer> call(String[] array) throws Exception {
                    return new Tuple2<>(new Integer(array[7].trim()), new Integer(array[5].trim()));
                }
            }
        );
        JavaPairRDD<Integer, String> rdd4_4 = rdd4_2.mapToPair(
            new PairFunction<String[], Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(String[] array) throws Exception {
                    return new Tuple2<>(new Integer(array[0].trim()), array[2].trim());
                }
            }
        );

    /*
        val rdd4_5 = rdd4_3.join(rdd4_4)
    rdd4_5 = Array[(Int, (Int, String))] = Array(
      (30,(1600,CHICAGO)), (30,(1250,CHICAGO)), (30,(1250,CHICAGO)), (30,(2850,CHICAGO)), (30,(1500,CHICAGO)), (30,(950,CHICAGO)),
      (20,(800,DALLAS)), (20,(2975,DALLAS)), (20,(3000,DALLAS)),
      (10,(2450,NEW YORK)), (10,(5000,NEW YORK)), (10,(1300,NEW YORK))
    )
     */
        JavaPairRDD<Integer, Tuple2<Integer, String>> rdd4_5 =  rdd4_3.join(rdd4_4);



    /*
        val rdd4_6 = rdd4_5.map(a => a._2)
    rdd4_6 = Array[(Int, String)] = Array(
      (1600,CHICAGO), (1250,CHICAGO), (1250,CHICAGO), (2850,CHICAGO), (1500,CHICAGO), (950,CHICAGO),
      (800,DALLAS), (2975,DALLAS), (3000,DALLAS),
      (2450,NEW YORK), (5000,NEW YORK), (1300,NEW YORK)
    )

        val rdd4_7 = rdd4_6.map(a => (a._2, a._1))
    rdd4_7: Array[(String, Int)] = Array(
      (CHICAGO,1600), (CHICAGO,1250), (CHICAGO,1250), (CHICAGO,2850), (CHICAGO,1500), (CHICAGO,950),
      (DALLAS,800), (DALLAS,2975), (DALLAS,3000),
      (NEW YORK,2450), (NEW YORK,5000), (NEW YORK,1300)
    )

        val rdd4_8 = rdd4_7.reduceByKey(_+_)
    rdd4_8: Array[(String, Int)] = Array((CHICAGO,9400), (NEW YORK,8750), (DALLAS,6775))
    */

        JavaPairRDD<String, Integer> rdd4_7 =  rdd4_5.mapToPair(
            new PairFunction<Tuple2<Integer, Tuple2<Integer, String>>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<Integer, Tuple2<Integer, String>> a) throws Exception {
                    return new Tuple2<String, Integer>(a._2()._2(), a._2()._1());
                }
            }
        );


        JavaPairRDD<String, Integer> rdd4_8 = rdd4_7.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer a, Integer b) throws Exception {
                    return a+b;
                }
            }
        );

        System.out.println(rdd4_8.toDebugString());
        rdd4_8.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query04 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query04 hdfs://quickstart.cloudera:8020/user/cloudera/class/q4
