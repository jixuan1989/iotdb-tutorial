package cn.edu.thu.collect;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TsFileDirectly implements Sender  {

  TsFileWriter tsFileWriter;

  @Override
  public void connect(String ip, int port) throws Exception {
    String path = ip + "_" + port + ".tsfile";
    File f = FSFactoryProducer.getFSFactory().getFile(path);
    if (f.exists()) {
      f.delete();
    }
    tsFileWriter = new TsFileWriter(f);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

  }

  public void register(String measurement, TSDataType type, TSEncoding encoding)
      throws WriteProcessException {
    tsFileWriter.addMeasurement(new MeasurementSchema(measurement, type, encoding));
  }

  @Override
  public void register(String command) throws Exception {
    // add measurements into file schema
    throw new Exception("can not be used");
  }

  @Override
  public void write(String command) throws Exception {
    throw new Exception("can not be used");
  }

  @Override
  public void close() {
    try {
      tsFileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void write(String device, String measurement, long time, double value)
      throws Exception {
    //writing data point by point is not so good for the performance. But we have to do here
    //for fitting the interface..
    TSRecord tsRecord = new TSRecord(time, device);
    DataPoint dPoint1 = new DoubleDataPoint(measurement, value);
    tsRecord.addTuple(dPoint1);
    tsFileWriter.write(tsRecord);
  }
}
