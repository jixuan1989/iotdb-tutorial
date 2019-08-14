/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.thu.analyze;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.iotdb.tsfile.*;

public class SparkSQLAnalyzer {

  SparkSession spark;

  public static void main(String[] args) {
    SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer();
    analyzer.readFile();
  }

  public SparkSQLAnalyzer() {
    spark = SparkSession
        .builder()
        .appName("Java Spark SQL TsFile example")
        .master("local[*]") // if the spark is on remote server, change this to the master url.
        .config("spark.some.config.option", "some-value")
        .getOrCreate();
  }

  public void readFile() {
    Dataset<Row> df = spark.read().format("org.apache.iotdb.tsfile").load("/Users/hxd/Documents/git/incubator-iotdb/server/target/iotdb-server-0.9.0-SNAPSHOT/data/data/sequence/root.app/1565762377311-101.tsfile");
    df.show();

    df.createOrReplaceTempView("tsfile_table");
    Dataset<Row>  newDf = spark.sql("select count(*) from tsfile_table");
    newDf.show();
  }
}
