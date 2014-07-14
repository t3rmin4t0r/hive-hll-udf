package org.notmysock.hive;

import java.util.ArrayList;
import java.util.List;

import hyperloglog.HyperLogLog;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.notmysock.hive.UDAFHyperLogLog.HyperLogLogBuffer;
import org.notmysock.hive.UDAFHyperLogLog.HyperLogLogEvaluator;

@Description(name = "approx_distinct", value = "_FUNC_(x)")
public class UDAFApproximateDistinct extends UDAFHyperLogLog {
  
  public static final class CountApproximateDistinctEvaluator extends HyperLogLogEvaluator {
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      ObjectInspector hyperloglog = super.init(m, parameters);
      if(m == Mode.FINAL || m == Mode.COMPLETE) {
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      }
      return hyperloglog;
    }
    
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HyperLogLog hll = ((HyperLogLogBuffer)agg).hll;
      return new LongWritable(hll.count());
    }
  }
  
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new IllegalArgumentException("Function only takes 1 parameter");
    } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        && parameters[0].getCategory() != ObjectInspector.Category.STRUCT) {
      throw new UDFArgumentTypeException(1,
          "Only primitive/struct rows are accepted but "
              + parameters[0].getTypeName() + " was passed.");
    }
    return new CountApproximateDistinctEvaluator();
  }
}
