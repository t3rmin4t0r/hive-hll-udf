package org.notmysock.hive;

import hyperloglog.HyperLogLog;
import hyperloglog.HyperLogLog.EncodingType;
import hyperloglog.HyperLogLogUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "hyperloglog", value = "_FUNC_(x) - generates HyperLogLog structures from input column",
extended = "hive> create dau_table as select hyperloglog(uid) as hll_uid from traffic group by dt")
public class UDAFHyperLogLog implements GenericUDAFResolver2 {
  
  static final class HyperLogLogBuffer extends AbstractAggregationBuffer {
    public HyperLogLog hll;

    public HyperLogLogBuffer() {
      this.reset();
    }
    
    @Override
    public int estimate() {
      return 16*1024; /* 16kb usually */
    }

    public void reset() {
      hll = HyperLogLog.builder()
          .setNumRegisterIndexBits(15).setNumHashBits(64).setEncoding(EncodingType.SPARSE).build();
    }
  }
  
  public static class HyperLogLogEvaluator extends GenericUDAFEvaluator {

    ObjectInspector inputOI;
    WritableBinaryObjectInspector partialOI;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    
    /*
     * All modes returns BINARY columns.
     * 
     * PARTIAL1 takes in a primitive inspector
     * 
     * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#init(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode, org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector[])
     */
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      partialOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      switch (m) {
      case PARTIAL1: 
        inputOI = parameters[0];
      case PARTIAL2:
        return partialOI;
      case FINAL:
      case COMPLETE:
        return partialOI;
      default:
        throw new IllegalArgumentException("Unknown UDAF mode " + m);
      }
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new HyperLogLogBuffer();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] args)
        throws HiveException {
      if (args[0] == null) {
        return;
      }
      HyperLogLog hll = ((HyperLogLogBuffer)agg).hll;
      // should use BinarySortableSerDe, perhaps
      Object val = ObjectInspectorUtils.copyToStandardJavaObject(args[0], inputOI);
      try {
        if (val instanceof Byte || val instanceof Character || val instanceof Short) {
          hll.add(val.hashCode());
        } else if (val instanceof Integer) {
          hll.addInt(((Integer) val).intValue());
        } else if(val instanceof Long) {
          hll.addLong(((Long) val).longValue());
        } else if (val instanceof Float) {
          hll.addFloat(((Float) val).floatValue());
        } else if (val instanceof Double) {
          hll.addDouble((Double)val);
        } else if (val instanceof String) {
          hll.addString(val.toString());
        } else {
          /* potential multi-key option */
          output.reset();
          ObjectOutputStream out = new ObjectOutputStream(output);
          out.writeObject(val);
          byte[] key = output.toByteArray();
          hll.addBytes(key);
        }
      } catch(IOException ioe) {
        throw new HiveException(ioe);
      }
    }
    
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      HyperLogLog hll = ((HyperLogLogBuffer)agg).hll;
      output.reset();
      try {
        HyperLogLogUtils.serializeHLL(output, hll);
      } catch(IOException ioe) {
        throw new HiveException(ioe);
      }
      return new BytesWritable(output.toByteArray());
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }
      final BytesWritable bw = partialOI.getPrimitiveWritableObject(partial);
      HyperLogLog hll = ((HyperLogLogBuffer)agg).hll;
      merge(hll, bw);
    }
    
    protected void merge(HyperLogLog hll, BytesWritable bw) throws HiveException {
      try {
        ByteArrayInputStream input = new ByteArrayInputStream(bw.getBytes(), 0, bw.getLength());
        HyperLogLog hll2 = HyperLogLogUtils.deserializeHLL(input);
        hll.merge(hll2);
        input.close();
      } catch (IOException ioe) {
        throw new HiveException(ioe);
      }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((HyperLogLogBuffer)agg).reset();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HyperLogLog hll = ((HyperLogLogBuffer)agg).hll;
      output.reset();
      try {
        HyperLogLogUtils.serializeHLL(output, hll);
      } catch(IOException ioe) {
        throw new HiveException(ioe);
      }
      return new BytesWritable(output.toByteArray());
    }
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
      throws SemanticException {
    return getEvaluator(info.getParameters());
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
    return new HyperLogLogEvaluator();
  }
}