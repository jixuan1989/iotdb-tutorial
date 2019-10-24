package cn.edu.thu.accessdata;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class TsFileQuery {

  public static void main(String[] args) throws IOException {
    ReadOnlyTsFile tsFileReader = new ReadOnlyTsFile(new TsFileSequenceReader("127.0.0.1_6667.tsfile"));
    // use these paths(all measurements) for all the queries
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("root.app.system.test_cpu"));

    // no filter, should select 1 2 3 4 6 7 8
    queryAndPrint(paths, tsFileReader, null);

    tsFileReader.close();
  }

  private static void queryAndPrint(ArrayList<Path> paths, ReadOnlyTsFile readTsFile, IExpression statement)
      throws IOException {
    QueryExpression.create();
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
  }
}
