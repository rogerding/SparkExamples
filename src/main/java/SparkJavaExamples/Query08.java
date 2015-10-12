package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by rding on 10/10/15.
 * (8) list 3 employee's name and salary with highest salary
 */
public class Query08 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";

    static int num_partition = 1;

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query08 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query08");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String[]> rdd8_1 = sc.textFile(file_1).map(
                new Function<String, String[]>() {
                    public String[] call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return parts;
                    }
                }
        );


    /*
        val rdd8_2 = rdd8_1.map(line => (line(5).trim.toInt, line(1).trim)).sortByKey(ascending = false)

    rdd8_2 = Array[(Int, String)] = Array(
      (5000,KING), (3000,FORD), (2975,JONES), (2850,BLAKE), (2450,CLARK), (1600,ALLEN),
      (1500,TURNER), (1300,MILLER), (1250,WARD), (1250,MARTIN), (950,JAMES), (800,SMITH)
    )

        val result = rdd8_2.map(a => (a._2, a._1)).take(3)


        sc.parallelize(result, num_partition).saveAsTextFile(args(0))

     */

        JavaPairRDD<Integer, String> rdd8_2 = rdd8_1.mapToPair(
                new PairFunction<String[], Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String[] array) throws Exception {
                        return new Tuple2<>(new Integer(array[5].trim()), array[1].trim());
                    }
                }
        ).sortByKey(false);

        List<Tuple2<Integer, String>> result = rdd8_2.take(3);

        System.out.println(rdd8_2.toDebugString());
        //result.saveAsTextFile(outPath);

        sc.parallelize(result, num_partition).saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query08 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query08 hdfs://quickstart.cloudera:8020/user/cloudera/class/q8
