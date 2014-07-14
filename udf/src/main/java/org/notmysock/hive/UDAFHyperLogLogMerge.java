package org.notmysock.hive;

import hyperloglog.HyperLogLog;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "hyperloglog_merge", value = "_FUNC_(x)")
public class UDAFHyperLogLogMerge extends UDAFHyperLogLog {
  public static final class HyperLogLogMergeEvaluator extends HyperLogLogEvaluator {
    @Override
    public void iterate(AggregationBuffer agg, Object[] args)
        throws HiveException {
      if (args[0] == null) {
        return;
      }
      final BytesWritable bw = ((BinaryObjectInspector) inputOI).getPrimitiveWritableObject(args[0]);
      HyperLogLog hll = ((HyperLogLogBuffer)agg).hll;
      merge(hll, bw);
    }
  }
  
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new IllegalArgumentException("Function only takes 1 parameter");
    } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE || 
        ((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory() != PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(1,
          "Only BINARY columns in HLL format are accepted but "
              + parameters[0].getTypeName() + " was passed.");
    }
    return new HyperLogLogMergeEvaluator();
  }
}