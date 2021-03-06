package SparkSQLJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by rding on 10/11/15.
 * (1) list total salary for each dept.
 */
public class SqlQ01 {

    static String file_1_parquet = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.parquet";

    static int num_partition = 1;

    static String empTableName = "emp";
    static String myQuery = "select deptno, sum(sal) as total from emp group by deptno";


    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Usage: SqlQ01 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-SqlQuery-01");
        // Performance tuning parameters
        sparkConf.set("spark.sql.codegen", "true"); // default is false
        sparkConf.set("spark.sql.inMemoryColumnarStorage.compressed", "true"); // default is false
        sparkConf.set("spark.sql.inMemoryColumnarStorage.batchSize", "500"); // default is 1000
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy"); // default is snappy, unconmpressed, gzip, lzo

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        // load data into DataFrame
        DataFrame df_emp = sqlContext.parquetFile(file_1_parquet);
//        df_emp.show();
//        df_emp.printSchema();
        df_emp.registerTempTable(empTableName);


        DataFrame result = sqlContext.sql(myQuery);
//        result.show();
//        result.printSchema();

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        JavaRDD<Row> rowRdd = result.toJavaRDD();
//        System.out.println(rowRdd.toDebugString());
//        System.out.println("rowRdd's partition number = " + rowRdd.partitions().size());

        // coalesce(numPartitions : scala.Int, shuffle : scala.Boolean)
        /*
        shuffle = false will merge partitions together, withOUT doing a more expensive shuffle operation.
        Each result partition just reads from a few of the input partitions

        shuffle = true, you trigger a full shuffle of the data, where each result partition depends on all input partitions.
        Its more expensive, but the advantages are:
        (a) if you want to increase the number of partitions, you have to do a shuffle
        (b) if some of your partitions are bigger (data skew) you might want to shuffle to balance them out more and
        (c) maybe you want to do the shuffle so the data is grouped, eg. for a subsequent join
         */
        JavaRDD<Row> rowRdd_co = rowRdd.coalesce(num_partition, false); // repartition to 1
//        System.out.println("rowRdd_co's partition number = " + rowRdd_co.partitions().size());

        rowRdd_co.saveAsTextFile(outPath);

//        // ROW can convert to JavaPairRDD
//        JavaPairRDD<Integer, Long> strRdd = rowRdd_co.mapToPair(
//            new PairFunction<Row, Integer, Long>() {
//                @Override
//                public Tuple2<Integer, Long> call(Row row) throws Exception {
//                    return new Tuple2<Integer, Long>(row.getInt(0), row.getLong(1));
//                }
//            }
//        );
//
//        System.out.println("strRdd's partition number = " + strRdd.partitions().size());
//
//
//        List<Tuple2<Integer, Long>> myList = strRdd.collect();
//
//        for (Tuple2<Integer, Long> t2 : myList) {
//            System.out.println("[" + t2._1() + "] => [" + t2._2() + "]");
//        }
//


        sc.close();
    }
}


// spark-submit --master yarn-client --class SparkSQLJava.SqlQ01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/q001
// spark-submit --master local[4] --class SparkSQLJava.SqlQ01 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/q001
