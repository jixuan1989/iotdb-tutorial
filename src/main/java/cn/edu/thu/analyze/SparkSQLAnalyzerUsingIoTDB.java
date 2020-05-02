package cn.edu.thu.analyze;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLAnalyzerUsingIoTDB {
  SparkSession spark;

  public static void main(String[] args) {
    SparkSQLAnalyzerUsingIoTDB analyzer = new SparkSQLAnalyzerUsingIoTDB();
    analyzer.readFile();
  }

  public SparkSQLAnalyzerUsingIoTDB() {
    spark = SparkSession
        .builder()
        .appName("Java Spark SQL IoTDB example")
        .master("local[*]") // if the spark is on remote server, change this to the master url.
        .config("spark.some.config.option", "some-value")
        .getOrCreate();
  }

  public void readFile() {
    Dataset<Row> df = spark.read().format("org.apache.iotdb.sparkdb")
        .option("url","jdbc:iotdb://127.0.0.1:6667/")
        .option("sql","select * from root").load();
    df.show(2000);


    //if you want to use narrow table, use:
    //df = Transformer.toNarrowForm(spark, df);

    df.createOrReplaceTempView("iotdb");
    Dataset<Row>  newDf = spark.sql("select count(*) from iotdb");
    newDf.show();
  }
}
