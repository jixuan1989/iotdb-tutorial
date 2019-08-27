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

import cn.edu.thu.collect.IoTDBDirectly;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMQXConsumerStore {

  Logger logger = LoggerFactory.getLogger(EMQXConsumerStore.class);
  private MqttClient client;
  private String topic;

  public static void main(String[] args) throws Exception {
    EMQXConsumerStore store = new EMQXConsumerStore("demo", "127.0.0.1", "1883", "127.0.0.1", 6667);
    store.doWork();
  }

  IoTDBDirectly ioTDBWriter = new IoTDBDirectly();

  public EMQXConsumerStore(String topic, String ip, String port, String iotdbIP, int iotdbPort)
      throws Exception {
    super();
    String host = "tcp://" + ip + ":" + port;
    this.topic = topic;
    ioTDBWriter.connect(iotdbIP, iotdbPort);
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    client = new MqttClient(host, "Client-"+System.currentTimeMillis());
    client.connect(options);
    client.setCallback(new PushCallback(ioTDBWriter));
  }

  private void doWork() throws MqttException {
    while (true) {
      client.subscribe(topic, 2);
    }
  }

}

class PushCallback implements MqttCallback {

  private IoTDBDirectly ioTDBWriter;

  public PushCallback(IoTDBDirectly ioTDBWriter) {
    this.ioTDBWriter = ioTDBWriter;
  }

  public void connectionLost(Throwable cause) {
  }

  public void deliveryComplete(IMqttDeliveryToken token) {
  }

  public void messageArrived(String topic, MqttMessage message) throws Exception {
    String sql = new String(message.getPayload());
    System.out.println("receive: " + sql);
    ioTDBWriter.write(sql);
  }
}
