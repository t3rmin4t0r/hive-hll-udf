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

@Description(name = "hyperloglog_value", value = "_FUNC_(x)")
public class UDFHyperLogLogValue extends UDF {
  public LongWritable evaluate(BytesWritable bw) throws HiveException {
    if (bw == null) {
      return new LongWritable(0);
    }
    ByteArrayInputStream input = new ByteArrayInputStream(bw.getBytes(), 0,
        bw.getLength());
    final HyperLogLog hll;
    try {
      hll = HyperLogLogUtils.deserializeHLL(input);
    } catch (IOException ioe) {
      throw new HiveException(ioe);
    }
    return new LongWritable(hll.count());
  }
}