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
 */
public class Query02 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";


    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query02 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query02");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaRDD<String> rdd1 = sc.textFile(file_1);

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

        //val rdd4 = rdd3.groupByKey()
        JavaPairRDD<Integer, Iterable<Integer>> rdd4 = rdd3.groupByKey();
    /*
    rdd4 = Array[(Int, Iterable[Int])] = Array(
              (30,ArrayBuffer(1600, 1250, 1250, 2850, 1500, 950)),
              (20,ArrayBuffer(800, 2975, 3000)),
              (10,ArrayBuffer(2450, 5000, 1300))
            )
     */

        //     val rdd5 = rdd4.mapValues(salary => (salary.size, 1.0 * salary.sum / salary.size))
         /*
    rdd5 = Array[(Int, (Int, Double))] = Array(
      (30,(6,1566.6666666666667)),
      (20,(3,2258.3333333333335)),
      (10,(3,2916.6666666666665))
    )
    */

        JavaPairRDD<Integer, Tuple2<Integer, Double>> rdd5 = rdd4.mapValues(
            new Function<Iterable<Integer>, Tuple2<Integer, Double>>() {
                @Override
                public Tuple2<Integer, Double> call(Iterable<Integer> integers) throws Exception {
                    int count = 0;
                    int sum = 0;
                    for(Integer cur : integers) {
                        count++;
                        sum += cur;
                    }
                    return new Tuple2<Integer, Double>(count, 1.0 * sum / count);
                }
            }
        );


        System.out.println(rdd5.toDebugString());
        rdd5.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query02 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query02 hdfs://quickstart.cloudera:8020/user/cloudera/class/q2
