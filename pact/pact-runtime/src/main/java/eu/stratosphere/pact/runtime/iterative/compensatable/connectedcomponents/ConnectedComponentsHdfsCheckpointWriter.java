package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.io.HdfsCheckpointWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

public class ConnectedComponentsHdfsCheckpointWriter extends HdfsCheckpointWriter {

  private final LongWritable key;
  private final LongWritable value;

  public ConnectedComponentsHdfsCheckpointWriter() throws IOException {
    key = new LongWritable();
    value = new LongWritable();
  }

  @Override
  protected Class<? extends WritableComparable> keyClass() {
    return key.getClass();
  }

  @Override
  protected Class<? extends Writable> valueClass() {
    return value.getClass();
  }

  @Override
  protected void add(SequenceFile.Writer writer, PactRecord record) throws IOException {
    key.set(record.getField(0, PactLong.class).getValue());
    value.set(record.getField(1, PactLong.class).getValue());
    writer.append(key, value);
  }
}
