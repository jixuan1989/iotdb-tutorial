this repo contains some codes to cover the life cycle of data in a simulated IoT scenario.

The process is:

- data generation ("datagenerator" package)
  - use JMX API to get the CPU and memory measurements of your computer.
  - (TODO) use PLC4J to get data from a modbus-protocol based PLC.

- data collection ("collect" package)
  - collect the above data into Kafka
  - collect the above data into MQTT server
  - collect the above data into IoTDB directly.
  
- data store
  - store all data into IoTDB
  
- data query ("accessdata" package)
  - query data from IoTDB.
  
  
- data analyze ("analyze" package)
  - analyze TsFile data that written by IoTDB.
    - To get one or more TsFiles, you may need to run an IoTDB-client.sh and execute "flush" command
     first. Then, you can copy the file path to `SparkSQLAnalyzer` file.

- data visualization
  - please read https://github.com/apache/incubator-iotdb/tree/master/grafana

    
## how to use EMQ 

* deploy EMQ: https://github.com/emqx/emqx/releases/tag/v4.0-alpha.1
* start EMQ: ./bin/emqx start
* deploy IoTDB: ./sbin/start-server.sh
* start IoTDB server: ./sbin/start
* run InitStorageGroup
* launch EMQXConsumerStore
* start DataGenerator
* start IoTDB client: ./sbin/start-client.sh
* select data: select * from root;