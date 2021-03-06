/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.thu.collect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class IoTDBDirectly implements Sender {

  protected Connection connection;
  protected Statement statement;

  @Override
  public void connect(String ip, int port) throws Exception {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    connection = java.sql.DriverManager
        .getConnection("jdbc:iotdb://" + ip + ":" + port + "/", "root", "root");
    statement = connection.createStatement();
  }

  @Override
  public void register(String command) throws Exception {
    try {
      statement.execute(command);
    } catch (SQLException e) {
      if (!e.getMessage().contains("exist")) {
        throw e;
      }
    }
  }

  @Override
  public void write(String command) throws Exception {
    statement.execute(command);
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }



}
