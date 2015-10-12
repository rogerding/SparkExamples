package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by rding on 10/10/15.
 * (7) list employee's name and dept name whose name start with "J"

 */
public class Query07 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";


    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query07 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query07");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String[]> rdd7_1 = sc.textFile(file_1).map(
                new Function<String, String[]>() {
                    public String[] call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return parts;
                    }
                }
        );

        JavaRDD<String[]> rdd7_2 = sc.textFile(file_2).map(
                new Function<String, String[]>() {
                    public String[] call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return parts;
                    }
                }
        );

    /*
        val rdd7_3 = rdd7_1.map(line => (line(7).trim.toInt, line(1).trim))
        val rdd7_4 = rdd7_2.map(line => (line(0).trim.toInt, line(1).trim))

    rdd7_3 = Array[(Int, String)] = Array(
      (20,SMITH), (30,ALLEN), (30,WARD), (20,JONES), (30,MARTIN), (30,BLAKE),
      (10,CLARK), (10,KING), (30,TURNER), (30,JAMES), (20,FORD), (10,MILLER)
    )

    rdd7_4 = Array[(Int, String)] = Array((10,ACCOUNTING), (20,RESEARCH), (30,SALES), (40,OPERATIONS))

        val result = rdd7_3.filter(_._2.startsWith("J")).join(rdd7_4).values

     */
        JavaPairRDD<Integer, String> rdd7_3 = rdd7_1.mapToPair(
                new PairFunction<String[], Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String[] array) throws Exception {
                        return new Tuple2<>(new Integer(array[7].trim()), array[1].trim());
                    }
                }
        );

        JavaPairRDD<Integer, String> rdd7_4 = rdd7_2.mapToPair(
                new PairFunction<String[], Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String[] array) throws Exception {
                        return new Tuple2<>(new Integer(array[0].trim()), array[1].trim());
                    }
                }
        );


        JavaPairRDD<Integer, Tuple2<String, String>> rdd7_5 =  rdd7_3.filter(
                new Function<Tuple2<Integer, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Integer, String> a) throws Exception {
                        return a._2().startsWith("J");
                    }
                }
        ).join(rdd7_4);

        /*
        (30,(JAMES,SALES))
        (20,(JONES,RESEARCH))
         */

        JavaRDD<Tuple2<String, String>> result = rdd7_5.values();

//        JavaPairRDD<String, String> result = rdd7_5.values().mapToPair(
//            new PairFunction<Tuple2<String, String>, String, String>() {
//                @Override
//                public Tuple2<String, String> call(Tuple2<String, String> a) throws Exception {
//                    return a;
//                }
//            }
//        );


        System.out.println(result.toDebugString());
        result.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query07 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query07 hdfs://quickstart.cloudera:8020/user/cloudera/class/q7
