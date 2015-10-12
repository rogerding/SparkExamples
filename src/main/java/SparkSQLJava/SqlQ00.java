package SparkSQLJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;

/**
 * Created by rding on 10/10/15.
 * (1) list total salary for each dept.
 */
public class SqlQ00 {

    static String file_1 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt";
    static String file_2 = "hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt";

    static int num_partition = 1;

    public static void main(String[] args)  {

        if (args.length != 1) {
            System.out.println("Usage: SqlQ00 outputPath");
            System.exit(1);
        }
        String outPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Java-SqlQuery-00");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        // Load a text file and convert each line to a Java Bean.
        JavaRDD<String> rdd1 = sc.textFile(file_1, num_partition);
        JavaRDD<Employee> rdd_emp = rdd1.map(
            new Function<String, Employee>() {
                @Override
                public Employee call(String line) throws Exception {
                    String[] parts = line.split(",");
                    Employee    emp = new Employee();
/* private String empno;
private String ename;
private String job;
private String mgr;
private String hiredate;
private int sal;
private int comm;
private int deptno;
*/
                    emp.setEmpno(parts[0]);
                    emp.setEname(parts[1]);
                    emp.setJob(parts[2]);
                    emp.setMgr(parts[3]);
                    emp.setHiredate(parts[4]);
                    emp.setSal(new Integer(parts[5]));
                    emp.setComm(new Integer(parts[6]));
                    emp.setDeptno(new Integer(parts[7]));

                    return emp;
                }
            }
        );

        // Apply a schema to an RDD of Java Beans and register it as a table.
        DataFrame schemaEmp = sqlContext.createDataFrame(rdd_emp, Employee.class);
        schemaEmp.registerTempTable("empTable");


        schemaEmp.show();

        schemaEmp.printSchema();

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame result = sqlContext.sql("select deptno, sum(sal) as total from empTable group by deptno");

        result.show();
        result.printSchema();

/*
deptno total
10     8750
20     6775
30     9400
*/


        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        JavaRDD<Row> rowRdd = result.toJavaRDD();
        System.out.println(rowRdd.toDebugString());
        System.out.println("rowRdd's partition number = " + rowRdd.partitions().size());

        // coalesce(numPartitions : scala.Int, shuffle : scala.Boolean)
        JavaRDD<Row> rowRdd_co = rowRdd.coalesce(num_partition, false); // repartition to 1
        System.out.println("rowRdd_co's partition number = " + rowRdd_co.partitions().size());

        rowRdd_co.saveAsTextFile(outPath);

        // ROW can convert to JavaPairRDD
        JavaPairRDD<Integer, Long> strRdd = rowRdd_co.mapToPair(
            new PairFunction<Row, Integer, Long>() {
                @Override
                public Tuple2<Integer, Long> call(Row row) throws Exception {
                    return new Tuple2<Integer, Long>(row.getInt(0), row.getLong(1));
                }
            }
        );

        System.out.println("strRdd's partition number = " + strRdd.partitions().size());


        List<Tuple2<Integer, Long>> myList = strRdd.collect();

        for (Tuple2<Integer, Long> t2 : myList) {
            System.out.println("[" + t2._1() + "] => [" + t2._2() + "]");
        }

        sc.stop();

    }
}


// spark-submit --master local[4] --class SparkSQLJava.SqlQ00 /home/cloudera/IdeaProjects/SparkExamples/target/SparkExamples-1.0-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/class/q00
