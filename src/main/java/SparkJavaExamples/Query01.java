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
 * (1) list total salary for each dept.
 */
public class Query01 {

    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";

    static int num_partition = 1;

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query01 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query01");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd1 = sc.textFile(file_1, num_partition);

        //val rdd2 = rdd1.map(_.split(","))
        JavaRDD<String[]> rdd2 = rdd1.map(
            new Function<String, String[]>() {
                public String[] call(String line) throws Exception {
                    String[] parts = line.split(",");
                    return parts;
                }
            }
        );

        // val rdd3 = rdd2.map(line => (line(7).trim.toInt, line(5).trim.toInt))
//        JavaRDD<Tuple2<Integer, Integer>> rdd3 = rdd2.map(
//            new Function<String[], Tuple2<Integer, Integer>>() {
//                public Tuple2<Integer, Integer> call(String[] array) throws Exception {
//                    return new Tuple2<Integer, Integer>(new Integer(array[7].trim()), new Integer(array[5].trim()));
//                }
//            }
//        );
//

        // Java 7 style
        JavaPairRDD<Integer, Integer> rdd3 = rdd2.mapToPair(
            new PairFunction<String[], Integer, Integer>() {
                @Override
                public Tuple2<Integer, Integer> call(String[] array) throws Exception {
                    return new Tuple2<Integer, Integer>(new Integer(array[7].trim()), new Integer(array[5].trim()));
                }
            }
        );

        /*
    rdd3 = Array[(Int, Int)] = Array(
      (20,800), (30,1600), (30,1250), (20,2975),
      (30,1250), (30,2850), (10,2450), (10,5000),
      (30,1500), (30,950), (20,3000), (10,1300))
     */

        // Java 8 style
        // JavaPairRDD<Integer, Integer> rdd3 = rdd2.mapToPair(line -> new Tuple2<Integer, Integer>(new Integer(line[7].trim()), new Integer(line[5].trim())));

        // val rdd4 = rdd3.reduceByKey(_+_)
        //JavaPairRDD<Integer, Integer> rdd4 = rdd3.reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, Integer> rdd4 = rdd3.reduceByKey(
            new Function2<Integer, Integer, Integer>(){
                public Integer call(Integer x, Integer y){
                    return x + y;
                }
            }
        );

        System.out.println(rdd4.toDebugString());
        rdd4.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --master local[4] --class SparkJavaExamples.Query01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/q1
