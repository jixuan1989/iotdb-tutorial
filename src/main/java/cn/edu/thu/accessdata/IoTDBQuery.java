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

package cn.edu.thu.accessdata;

import cn.edu.thu.collect.IoTDBDirectly;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class IoTDBQuery  extends IoTDBDirectly {

  public static void main(String[] args) throws Exception {
    IoTDBQuery query = new IoTDBQuery();
    query.connect("127.0.0.1", 6667);
    query.query();
    query.close();
  }


  public void query() throws SQLException {
    ResultSet resultSet = statement.executeQuery("select * from root.app");
    System.out.println("Timestamp\t|\t value\t|\t name");
    while(resultSet.next()) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int total = resultSetMetaData.getColumnCount();
      long timestamp = resultSet.getLong(1);
      for (int i = 2; i < total; i++) {
        if(resultSet.getObject(i) != null) {
          System.out.println(String.format("%s\t|\t %s\t|\t %s", timestamp,  resultSet.getObject(i), resultSetMetaData.getColumnName(i)));
        }
      }
    }
    System.out.println("finished");
  }
}
