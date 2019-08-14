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

package cn.edu.thu.store;

import cn.edu.thu.collect.EMQXSender;
import cn.edu.thu.collect.IoTDBDirectly;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMQXConsumerStore extends EMQXSender {

  Logger logger = LoggerFactory.getLogger(EMQXConsumerStore.class);
  public static void main(String[] args) throws Exception {
    EMQXConsumerStore consumerStore = new EMQXConsumerStore("demo", "127.0.0.1", 9092, "127.0.0.1", 6667);
    consumerStore.doWork();
  }

  IoTDBDirectly ioTDBWriter = new IoTDBDirectly();

  public EMQXConsumerStore(String topic, String ip, int port, String iotdbIP, int iotdbPort)
      throws Exception {
    super();
    this.topic = topic;
    this.connect(ip, port);
    ioTDBWriter.connect(iotdbIP, iotdbPort);
  }

  public void doWork() throws MqttException {
    sampleClient.subscribe(topic);

    sampleClient.setCallback(new MqttCallback() {
      public void messageArrived(String topic, MqttMessage message) throws Exception {
        String sql = new String(message.getPayload());
        ioTDBWriter.write(sql);
      }

      public void deliveryComplete(IMqttDeliveryToken token) {
      }

      public void connectionLost(Throwable throwable) {
      }
    });
    System.out.println("waiting for receiving data...");

  }


}
