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

package cn.edu.thu.datagenerator;

import cn.edu.thu.collect.EMQXSender;
import cn.edu.thu.collect.IoTDBDirectly;
import cn.edu.thu.collect.Sender;
import cn.edu.thu.collect.TsFileDirectly;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.ListenerNotFoundException;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//modified from https://github.com/xqbase/metric/blob/master/client/src/main/java/com/xqbase/metric/client/ManagementMonitor.java

public class DataGenerator implements Runnable {
  Logger logger = LoggerFactory.getLogger(DataGenerator.class);

  public static void main(String[] args) throws Exception {

    Sender writer = null;

    //writer = new IoTDBDirectly();
    writer = new TsFileDirectly();
    writer.connect("127.0.0.1", 6667);

    //writer = new EMQXSender();
    //writer.connect("127.0.0.1", 1883);

    //writer = new KafkaSender();
    //writer.connect("127.0.0.1", 9092);

    DataGenerator monitor = new DataGenerator("test", writer);

    //begin to collect data.

    ExecutorService service = Executors.newSingleThreadScheduledExecutor();
    ((ScheduledExecutorService) service)
        .scheduleWithFixedDelay(monitor, 0, 1000, TimeUnit.MILLISECONDS);

    while (true) {
      //collect data forever...
    }
  }

  //when creating time series finished, the field will be set as true.
  boolean init = false;

  //where to collect data, kafka or EMQXSender temporarily, or IoTDB directly.
  Sender writer;

  private void put(String name, double value, String... tagPairs) {
    if (tagPairs.length == 2) {
      System.out.println(tagPairs[0] + "=" + tagPairs[1] + "," + name + " \t" + value);
    } else if (tagPairs.length == 4) {
      System.out.println(
          tagPairs[0] + "=" + tagPairs[1] + "," + tagPairs[2] + "=" + tagPairs[3] + "," + name
              + " \t" + value);
    }
    String device = getPath(tagPairs);
    String path = device + "." + name;

    if (!init) {
      //create timeseries;
      String sql = "create timeseries " + path + " with DATATYPE=DOUBLE,ENCODING=RLE";
      System.err.println(sql);

      try {
        if (writer != null) {
          if (writer instanceof TsFileDirectly) {
            //tsfile does not need SQL
            ((TsFileDirectly)writer).register(name, TSDataType.DOUBLE, TSEncoding.RLE);
          } else {
            //using SQL
            writer.register(sql);
          }
        }
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    }

    try {
      //insert data
      if (writer != null) {
        if (writer instanceof  TsFileDirectly) {
          ((TsFileDirectly)writer).write(device, name, time, value);
        } else {
          String sql = String.format("insert into %s (timestamp, %s) values (%d, %f);", device, name,
              time, value);
          writer.write(sql);
          System.err.println(sql);
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }

  }


  private String getPath(String... tagPairs) {
    switch (tagPairs.length) {
      case 2:
        return "root.app." + tagPairs[1].replaceAll(" ", "_");
      case 4:
        return "root.app." + tagPairs[3].replaceAll(" ", "_") + "." + tagPairs[1]
            .replaceAll(" ", "_");
      default:
        return "root.app";
    }
  }


  private static double MB(long value) {
    return (double) value / 1048576;
  }

  private static double PERCENT(long dividend, long divisor) {
    return divisor == 0 ? 0 : (double) dividend * 100 / divisor;
  }


  private String cpu, threads, memoryMB, memoryPercent;
  private String memoryPoolMB, memoryPoolPercent;
  private Map<String, String> tagMap;
  private ThreadMXBean thread = ManagementFactory.getThreadMXBean();
  private MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
  private List<MemoryPoolMXBean> memoryPools =
      ManagementFactory.getMemoryPoolMXBeans();
  private OperatingSystemMXBean os = null;
  private Map<NotificationBroadcaster, NotificationListener>
      gcListeners = new HashMap<>();

  public DataGenerator(String prefix, Sender writer) {
    this.writer = writer;
    cpu = prefix + "_cpu";
    threads = prefix + "_threads";
    memoryMB = prefix + "_memory_mb";
    memoryPercent = prefix + "_memory_percent";
    memoryPoolMB = prefix + "_memory_pool_mb";
    memoryPoolPercent = prefix + "_memory_pool_percent";
    java.lang.management.OperatingSystemMXBean os_ =
        ManagementFactory.getOperatingSystemMXBean();
    if (os_ instanceof OperatingSystemMXBean) {
      os = (OperatingSystemMXBean) os_;
    }

    String gc = prefix + "_gc_time";
    for (GarbageCollectorMXBean gcBean :
        ManagementFactory.getGarbageCollectorMXBeans()) {
      if (!(gcBean instanceof NotificationBroadcaster)) {
        continue;
      }
      String gcName = gcBean.getName();
      NotificationListener listener = (notification, handback) -> {
        if (gcBean instanceof com.sun.management.GarbageCollectorMXBean) {
          put(gc, ((com.sun.management.GarbageCollectorMXBean) gcBean).
              getLastGcInfo().getDuration(), "name", gcName);
        }
      };
      NotificationBroadcaster broadcaster = ((NotificationBroadcaster) gcBean);
      broadcaster.addNotificationListener(listener, null, null);
      gcListeners.put(broadcaster, listener);
    }
  }

  long time;
  public void run() {
    time =System.currentTimeMillis();
    put(threads, thread.getThreadCount(), "type", "total");
    put(threads, thread.getDaemonThreadCount(), "type", "daemon");

    // Runtime rt = Runtime.getRuntime();
    // add(memory, MB(rt.totalMemory() - rt.freeMemory()), "type", "heap_used");
    MemoryUsage heap = memory.getHeapMemoryUsage();
    put(memoryMB, MB(heap.getCommitted()), "type", "heap_committed");
    long heapUsed = heap.getUsed();
    put(memoryMB, MB(heapUsed), "type", "heap_used");
    long heapMax = heap.getMax();
    put(memoryMB, MB(heapMax), "type", "heap_max");
    put(memoryPercent, PERCENT(heapUsed, heapMax), "type", "heap");
    MemoryUsage nonHeap = memory.getNonHeapMemoryUsage();
    put(memoryMB, MB(nonHeap.getCommitted()), "type", "non_heap_committed");
    long nonHeapUsed = nonHeap.getUsed();
    put(memoryMB, MB(nonHeapUsed), "type", "non_heap_used");
    long nonHeapMax = nonHeap.getMax();
    if (nonHeapMax > 0) {
      put(memoryMB, MB(nonHeapMax), "type", "non_heap_max");
      put(memoryPercent, PERCENT(nonHeapUsed, nonHeapMax), "type", "non_heap");
    }

    for (MemoryPoolMXBean memoryPool : memoryPools) {
      String poolName = memoryPool.getName();
      MemoryUsage pool = memoryPool.getUsage();
      if (pool == null) {
        continue;
      }
      put(memoryPoolMB, MB(pool.getCommitted()), "type", "committed", "name", poolName);
      long poolUsed = pool.getUsed();
      put(memoryPoolMB, MB(poolUsed), "type", "used", "name", poolName);
      long poolMax = pool.getMax();
      if (poolMax > 0) {
        put(memoryPoolMB, MB(poolMax), "type", "max", "name", poolName);
        put(memoryPoolPercent, PERCENT(poolUsed, poolMax), "name", poolName);
      }
    }

    if (os == null) {
      return;
    }
    long totalPhysical = os.getTotalPhysicalMemorySize();
    put(memoryMB, MB(totalPhysical), "type", "physical_total");
    long usedPhysical = totalPhysical - os.getFreePhysicalMemorySize();
    put(memoryMB, MB(usedPhysical), "type", "physical_used");
    put(memoryPercent, PERCENT(usedPhysical, totalPhysical), "type", "physical");
    long totalSwap = os.getTotalSwapSpaceSize();
    put(memoryMB, MB(totalSwap), "type", "swap_total");
    long usedSwap = totalSwap - os.getFreeSwapSpaceSize();
    put(memoryMB, MB(usedSwap), "type", "swap_used");
    put(memoryPercent, PERCENT(usedSwap, totalSwap), "type", "swap");
    put(memoryMB, MB(os.getCommittedVirtualMemorySize()),
        "type", "process_committed");

    put(cpu, Math.max(os.getSystemCpuLoad() * 100, 0), "type", "system");
    put(cpu, Math.max(os.getProcessCpuLoad() * 100, 0), "type", "process");

    init = true;
  }


  public void close() {
    gcListeners.forEach((broadcaster, listener) -> {
      try {
        broadcaster.removeNotificationListener(listener);
      } catch (ListenerNotFoundException e) {
        // Ignored
      }
    });
    gcListeners.clear();
  }

}
