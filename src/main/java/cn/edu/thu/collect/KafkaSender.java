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

package cn.edu.thu.collect;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaSender implements Sender {
  private  String topic = "demo";
  private  Boolean isAsync = false;
  KafkaProducer producer;
  int messageNo = 1;

  @Override
  public void connect(String ip, int port) throws Exception{
    Properties kafkaProps = new Properties();
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + port);
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<String, String>(kafkaProps);
  }

  @Override
  public void register(String command)  throws Exception{
    write(command);
  }

  @Override
  public void write(String command) throws Exception {
    long startTime = System.currentTimeMillis();
    if (isAsync) { // Send asynchronously
      producer.send(new ProducerRecord<>(topic, messageNo, command),
          new DemoCallBack(startTime, messageNo, command));
    } else { // Send synchronously
      try {
        producer.send(new ProducerRecord<>(topic, messageNo, command)).get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    ++messageNo;
  }

  @Override
  public void close() {
    producer.close();
  }


  class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
      this.startTime = startTime;
      this.key = key;
      this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request
     * completion. This method will be called when the record sent to the server has been
     * acknowledged. When exception is not null in the callback, metadata will contain the special
     * -1 value for all fields except for topicPartition, which will be valid.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     * Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error
     * occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      if (metadata != null) {
        System.out.println(
            "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                "), " +
                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
      } else {
        exception.printStackTrace();
      }
    }
  }


}
