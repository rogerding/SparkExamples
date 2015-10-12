package SparkSQLJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by rding on 10/11/15.
 */
public class SqlQ06 {

    static String file_1_parquet = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.parquet";
    static String file_2_parquet = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.parquet";

    static int num_partition = 1;

    static String empTableName = "emp";
    static String deptTableName = "dept";
    static String myQuery = "select b.ename, b.sal from (select avg(sal) AS avgsal from emp) a JOIN emp b where b.sal > a.avgsal order by b.sal";

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Usage: SqlQ06 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-SqlQuery-06");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);


        DataFrame df_emp = sqlContext.parquetFile(file_1_parquet);
        df_emp.show();
        df_emp.printSchema();
        df_emp.registerTempTable(empTableName);

//        DataFrame df_dept = sqlContext.parquetFile(file_2_parquet);
//        df_dept.registerTempTable(deptTableName);

        DataFrame result = sqlContext.sql(myQuery);
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

        // ROW can convert to JavaRDD
//        JavaRDD<Tuple3<Integer, Long, Double>> strRdd = rowRdd_co.map(
//                new Function<Row, Tuple3<Integer, Long, Double>>() {
//                    @Override
//                    public Tuple3<Integer, Long, Double> call(Row row) throws Exception {
//                        return new Tuple3<Integer, Long, Double>(row.getInt(0), row.getLong(1), row.getDouble(2));
//                    }
//                }
//
//        );
//
//        System.out.println("strRdd's partition number = " + strRdd.partitions().size());
//
//
//        List<Tuple3<Integer, Long, Double>> myList = strRdd.collect();
//
//        for (Tuple3<Integer, Long, Double> t3 : myList) {
//            System.out.println("[" + t3._1() + ", " + t3._2() + ", " + t3._3() + "]");
//        }

        sc.close();
    }
}


// spark-submit --master yarn-client --class SparkSQLJava.SqlQ06 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/q006
// spark-submit --master local[4] --class SparkSQLJava.SqlQ06 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/q006
