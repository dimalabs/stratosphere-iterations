package eu.stratosphere.pact.runtime.iterative.io;

import eu.stratosphere.pact.common.stubs.Collector;

import java.io.IOException;

public interface DataOutputCollector<T> extends Collector<T> {

  void prepare() throws IOException;

  long getElementsCollectedAndReset() throws IOException;

}
