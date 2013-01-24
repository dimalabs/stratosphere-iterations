package eu.stratosphere.pact.runtime.iterative.io;

import com.google.common.io.Closeables;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;

import java.io.IOException;

public class CheckpointingDataOutputCollector<T extends Record> implements DataOutputCollector<T> {

  private final Class<? extends HdfsCheckpointWriter> checkpointWriterClass;
  private final String checkpointPath;
  private String jobID;
  private final int workerIndex;

  private final DataOutputCollector<T> delegate;

  private int iteration;
  private HdfsCheckpointWriter checkpointWriter;

  public CheckpointingDataOutputCollector(Class<? extends HdfsCheckpointWriter> checkpointWriterClass, String jobID,
      int workerIndex, String checkpointPath, DataOutputCollector<T> delegate) {
    this.checkpointWriterClass = checkpointWriterClass;
    this.jobID = jobID;
    this.workerIndex = workerIndex;
    this.checkpointPath = checkpointPath;
    this.delegate = delegate;
    iteration = 1;
  }

  @Override
  public void prepare() throws IOException {

    try {
      checkpointWriter = checkpointWriterClass.asSubclass(HdfsCheckpointWriter.class).newInstance();
    } catch (Exception e) {
      throw new IOException("Unable to instantiate checkpointWriter " + checkpointWriterClass, e);
    }

    String path = checkpointPath + "/" + jobID +"/iteration-" + iteration + "/" + workerIndex;

    checkpointWriter.open(path);

    iteration++;

    delegate.prepare();
  }

  @Override
  public long getElementsCollectedAndReset() throws IOException {
    Closeables.closeQuietly(checkpointWriter);
    return delegate.getElementsCollectedAndReset();
  }

  @Override
  public void collect(T record) {
    try {
    //TODO type safety
    checkpointWriter.addToCheckpoint((PactRecord) record);
    } catch (IOException e) {
      throw new RuntimeException("Error while simulating checkpointing", e);
    }
    delegate.collect(record);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
