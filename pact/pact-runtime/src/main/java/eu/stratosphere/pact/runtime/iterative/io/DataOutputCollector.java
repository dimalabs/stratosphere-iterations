/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.io;

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;

import java.io.IOException;

/** {@link Collector} to write to a {@link DataOutputView} */
public class DataOutputCollector<T> implements Collector<T> {

  /** {@link DataOutputView} to write to */
  private final DataOutputView outputView;
  /** serializer to use */
  private final TypeSerializer<T> typeSerializer;

  public DataOutputCollector(DataOutputView outputView, TypeSerializer<T> typeSerializer) {
    this.outputView = outputView;
    this.typeSerializer = typeSerializer;
  }

  @Override
  public void collect(T record) {
    try {
      typeSerializer.serialize(record, outputView);
    }
    catch (IOException e) {
      throw new RuntimeException("Unable to serialize the record", e);
    }
  }

  @Override
  public void close() {}
}
