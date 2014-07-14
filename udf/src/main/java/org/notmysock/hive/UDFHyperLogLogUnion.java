package org.notmysock.hive;

import hyperloglog.HyperLogLog;
import hyperloglog.HyperLogLogUtils;
import hyperloglog.HyperLogLog.HyperLogLogBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.notmysock.hive.UDAFHyperLogLog.HyperLogLogBuffer;

public class UDFHyperLogLogUnion extends UDF {
  public BytesWritable evaluate(BytesWritable... args) throws HiveException {
    HyperLogLog hll = null;

    for (BytesWritable bw : args) {
      if (bw != null) {
        HyperLogLog hll2 = deserialize(bw);
        if(hll == null) {
          hll = hll2;
        } else {
          hll.merge(hll2);
        }
      }
    }
    return serialize(hll);
  }
  
  private final BytesWritable serialize(HyperLogLog hll) throws HiveException {
    if(hll == null) {
      return null;
    }
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.reset();
    try {
      HyperLogLogUtils.serializeHLL(output, hll);
    } catch(IOException ioe) {
      throw new HiveException(ioe);
    }
    return new BytesWritable(output.toByteArray());
  }

  private final HyperLogLog deserialize(BytesWritable bw) throws HiveException {
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(bw.getBytes(), 0,
          bw.getLength());
      return HyperLogLogUtils.deserializeHLL(input);
    } catch (IOException ioe) {
      throw new HiveException(ioe);
    }
  }
}
