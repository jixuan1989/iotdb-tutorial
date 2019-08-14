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
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerStore {
  Logger logger = LoggerFactory.getLogger(KafkaConsumerStore.class);
  public static void main(String[] args) throws Exception {
    KafkaConsumerStore consumerStore = new KafkaConsumerStore("demo", "127.0.0.1", 9092, "127.0.0.1", 6667);
    consumerStore.doWork();
  }

  private final KafkaConsumer<Integer, String> consumer;
  private final String topic;

  IoTDBDirectly ioTDBWriter = new IoTDBDirectly();

  public KafkaConsumerStore(String topic, String ip, int port, String iotdbIp, int iotdbPort)
      throws Exception {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + port);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    consumer = new KafkaConsumer<>(props);
    this.topic = topic;
    ioTDBWriter.connect(iotdbIp, iotdbPort);
  }

  public void doWork() {
    consumer.subscribe(Collections.singletonList(this.topic));
    while (true) {
      ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<Integer, String> record : records) {
        try {
          System.err.println(record.value());
          ioTDBWriter.write(record.value());
        } catch (Exception e) {
          logger.error(e.getMessage());
        }
      }
    }
  }


}
