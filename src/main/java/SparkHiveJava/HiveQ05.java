package SparkHiveJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

// to connect Spark SQL to an existing Hive installation, copy hive-site.xml to Spark's conf dir.

/**
 * Created by rding on 10/10/15.
 * (5) list employee's name and salary whose salary is higher than their manager
 */
public class HiveQ05 {

    static int num_partition = 1;
    static String myQuery1 = "use emp";
    static String myQuery2 = "select e.ename, e.sal from emp e JOIN emp e2 ON e.mgr = e2.empno where e.sal > e2.sal";

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: HiveQ05 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-HiveQuery-05");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Create HiveContext
        SQLContext sqlContext = new HiveContext(sc.sc());

        sqlContext.sql(myQuery1);
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


// spark-submit --master local[4] --class SparkHiveJava.HiveQ05 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/h05
