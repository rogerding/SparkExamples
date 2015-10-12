package SparkHiveJava;

import SparkSQLJava.Employee;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.List;

// to connect Spark SQL to an existing Hive installation, copy hive-site.xml to Spark's conf dir.
/**
 * Created by rding on 10/10/15.
 * (1) list total salary for each dept.
 */
public class HiveQ01 {

    static int num_partition = 1;
    static String myQuery1 = "use emp";
    static String myQuery2 = "select deptno, sum(sal) as total from emp.emp group by deptno";

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: HiveQ01 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-HiveQuery-01");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        SQLContext sqlContext = new HiveContext(sc.sc());

        //sqlContext.sql(myQuery1);
        DataFrame result = sqlContext.sql(myQuery2);
        result.show();
        result.printSchema();

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        JavaRDD<Row> rowRdd = result.toJavaRDD();
        System.out.println(rowRdd.toDebugString());
        System.out.println("rowRdd's partition number = " + rowRdd.partitions().size());

        // coalesce(numPartitions : scala.Int, shuffle : scala.Boolean)
        JavaRDD<Row> rowRdd_co = rowRdd.coalesce(num_partition, false); // repartition to 1
        System.out.println("rowRdd_co's partition number = " + rowRdd_co.partitions().size());

        rowRdd_co.saveAsTextFile(outPath);


        sc.stop();

    }
}


// spark-submit --master local[4] --class SparkHiveJava.HiveQ01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/h01
