package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

/**
 * Created by rding on 10/10/15.
 * (9) sort employee by total income (salary+comm), list name and total income.

 */
public class Query09 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";

    static int num_partition = 1;

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query09 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query09");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaRDD<String[]> rdd9_1 = sc.textFile(file_1).map(
            new Function<String, String[]>() {
                public String[] call(String line) throws Exception {
                    String[] parts = line.split(",");
                    return parts;
                }
            }
        );


    /*
        val rdd9_2 = rdd9_1.map(line => (line(5).trim.toInt, line(6).trim, line(1).trim))

    val rdd9_3 = rdd9_2.map(combineIncome)

     rdd9_3 = Array[(Int, String)] = Array(
        (800,SMITH), (1900,ALLEN), (1750,WARD), (2975,JONES), (2650,MARTIN), (2850,BLAKE),
        (2450,CLARK), (5000,KING), (1500,TURNER), (950,JAMES), (3000,FORD), (1300,MILLER)
      )


        val result = rdd9_3.sortByKey(ascending = false).map(a => (a._2, a._1))

        */

        JavaRDD<Tuple3<Integer, String, String>> rdd9_2 = rdd9_1.map(
            new Function<String[], Tuple3<Integer, String, String>>() {
                @Override
                public Tuple3<Integer, String, String> call(String[] array) throws Exception {
                    return new Tuple3<>(new Integer(array[5].trim()), array[6].trim(), array[1].trim());
                }
            }
        );

        JavaPairRDD<Integer, String> rdd9_3 = rdd9_2.mapToPair(
            new PairFunction<Tuple3<Integer, String, String>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple3<Integer, String, String> a) throws Exception {
                    if (a._2().length() > 0) {
                        return new Tuple2<Integer, String>(a._1() + new Integer(a._2()), a._3());
                    } else {
                        return new Tuple2<Integer, String>(a._1(), a._3());
                    }
                }
            }
        );

        JavaPairRDD<Integer, String> result = rdd9_3.sortByKey(false);

        System.out.println(result.toDebugString());
        result.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query09 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query09 hdfs://quickstart.cloudera:8020/user/cloudera/class/q9
