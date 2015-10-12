package SparkJavaExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by rding on 10/10/15.
 *  * (6) list employee's name and salary whose salary is higher than average salary of whole company

 */
public class Query06 {
    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";


    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: Query06 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-Query06");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaRDD<String[]> rdd6_1 = sc.textFile(file_1).map(
                new Function<String, String[]>() {
                    public String[] call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return parts;
                    }
                }
        );

        //val salRdd = rdd6_2.map(line => line(5).trim.toInt).cache()
        //val avgSal = salRdd.sum / salRdd.count    // 2077.0833333333335

        JavaRDD<Double> salRdd0 = rdd6_1.map(
                new Function<String[], Double>() {
                    @Override
                    public Double call(String[] array) throws Exception {
                        return new Double(array[5].trim());
                    }
                }
        );

        JavaDoubleRDD salRdd = rdd6_1.mapToDouble(
            new DoubleFunction<String[]>() {
                @Override
                public double call(String[] array) throws Exception {
                    return new Double(array[5].trim());
                }
            }
        );
        final double avgSal = salRdd.sum() / salRdd.count();


 /*
        // val rdd6_3 = rdd6_2.map(line => (line(1).trim, line(5).trim.toInt, avgSal))
    rdd6_3 = Array[(String, Int, Double)] = Array(
      (SMITH,800,2077.0833333333335), (ALLEN,1600,2077.0833333333335), (WARD,1250,2077.0833333333335),
      (JONES,2975,2077.0833333333335), (MARTIN,1250,2077.0833333333335), (BLAKE,2850,2077.0833333333335),
      (CLARK,2450,2077.0833333333335), (KING,5000,2077.0833333333335), (TURNER,1500,2077.0833333333335),
      (JAMES,950,2077.0833333333335), (FORD,3000,2077.0833333333335), (MILLER,1300,2077.0833333333335)
    )

        val rdd6_4 = rdd6_3.filter((a:(String, Int, Double)) => a._2 > a._3).map(a=>(a._1, a._2))

    rdd6_4 = Array[(String, Int)] = Array(
      (JONES,2975), (BLAKE,2850), (CLARK,2450),
      (KING,5000), (FORD,3000))

     */
        JavaRDD<Tuple3<String, Integer, Double>> rdd6_3 = rdd6_1.map(
            new Function<String[], Tuple3<String, Integer, Double>>() {
                @Override
                public Tuple3<String, Integer, Double> call(String[] array) throws Exception {
                    return new Tuple3<String, Integer, Double>(array[1].trim(), new Integer(array[5].trim()), avgSal);
                }
            }
        );

        JavaRDD<Tuple2<String, Integer>> rdd6_4 = rdd6_3.filter(
            new Function<Tuple3<String, Integer, Double>, Boolean>() {
                @Override
                public Boolean call(Tuple3<String, Integer, Double> a) throws Exception {
                    return a._2() > a._3();
                }
            }
        ).map(
            new Function<Tuple3<String, Integer, Double>, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> call(Tuple3<String, Integer, Double> a) throws Exception {
                    return new Tuple2<String, Integer>(a._1(), a._2());
                }
            }
        );


        System.out.println(rdd6_4.toDebugString());
        rdd6_4.saveAsTextFile(outPath);

        sc.stop();

    }
}

// spark-submit --class SparkJavaExamples.Query06 /home/cloudera/studyProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar yarn-client query06 hdfs://quickstart.cloudera:8020/user/cloudera/class/q6
