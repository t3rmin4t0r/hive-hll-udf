package org.notmysock.hive;
import hyperloglog.HyperLogLog;
import hyperloglog.HyperLogLogUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Description(name = "hyperloglog_debug", value = "_FUNC_(x)")
public class UDFHyperLogLogDebug extends UDF {
  public Text evaluate(BytesWritable bw) throws HiveException {
    if (bw == null) {
      return new Text("(null)");
    }
    ByteArrayInputStream input = new ByteArrayInputStream(bw.getBytes(), 0,
        bw.getLength());
    final HyperLogLog hll;
    try {
      hll = HyperLogLogUtils.deserializeHLL(input);
    } catch (IOException ioe) {
      throw new HiveException(ioe);
    }
    return new Text(hll.toStringExtended());
  }
}