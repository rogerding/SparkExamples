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
 * (3) list the first hired employee's name for each dept.
 */
public class Query03 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";


    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query03 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query03");
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


        // val rdd3 = rdd2.map(line => (line(7).trim.toInt, (line(1).trim, line(4).trim)))
 /*
    rdd3 = Array[(Int, (String, String))] = Array(
      (20,(SMITH,1980-12-17)), (30,(ALLEN,1981-02-20)), (30,(WARD,1981-02-22)),
      (20,(JONES,1981-04-02)), (30,(MARTIN,1981-09-28)), (30,(BLAKE,1981-05-01)),
      (10,(CLARK,1981-06-09)), (10,(KING,1981-11-17)), (30,(TURNER,1981-09-08)),
      (30,(JAMES,1981-12-03)), (20,(FORD,1981-12-03)), (10,(MILLER,1982-01-23))
     )

     */
        // Java 7 style
        JavaPairRDD<Integer, Tuple2<String, String>> rdd3 = rdd2.mapToPair(
            new PairFunction<String[], Integer, Tuple2<String, String>>() {
                @Override
                public Tuple2<Integer, Tuple2<String, String>> call(String[] array) throws Exception {
                    return new Tuple2<Integer, Tuple2<String, String>>
                        (new Integer(array[7].trim()), new Tuple2(new String(array[1].trim()), new String(array[4].trim())));
                }
            }
        );

        //val rdd4 = rdd3.reduceByKey(findEarlier)
    /*
    rdd4 = Array[(Int, (String, String))] = Array(
        (30,(ALLEN,1981-02-20)),
        (20,(SMITH,1980-12-17)),
        (10,(CLARK,1981-06-09))
    )
     */
        JavaPairRDD<Integer, Tuple2<String, String>> rdd4 = rdd3.reduceByKey(
            new Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, String> a, Tuple2<String, String> b) throws Exception {
                    String[] hireDate_a = a._2().split("-");
                    int year_a = Integer.parseInt(hireDate_a[0]);
                    int month_a = Integer.parseInt(hireDate_a[1]);
                    int day_a = Integer.parseInt(hireDate_a[2]);

                    String[] hireDate_b = b._2().split("-");
                    int year_b = Integer.parseInt(hireDate_b[0]);
                    int month_b = Integer.parseInt(hireDate_b[1]);
                    int day_b = Integer.parseInt(hireDate_b[2]);

                    if ( (year_a * 365 + month_a * 30 + day_a) <= (year_b * 365 + month_b * 30 + day_b) )
                        return a;
                    else
                        return b;
                }
            }
        );

        System.out.println(rdd4.toDebugString());
        rdd4.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query03 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query03 hdfs://quickstart.cloudera:8020/user/cloudera/class/q3
