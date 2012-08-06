/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.task.Terminable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * a delegating {@link MutableObjectIterator} that interrupts the current thread when a given number of events occured.
 * This is necessary to repetitively read channels when executing iterative data flows. The wrapped iterator must return false
 * on interruption, see {@link eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors}
 */
public class InterruptingMutableObjectIterator<E> implements MutableObjectIterator<E>, EventListener {

  private final MutableObjectIterator<E> delegate;
  private final String name;
  private final int numberOfEventsUntilInterrupt;
  private final AtomicInteger endOfSuperstepEventCounter;
  private final AtomicInteger terminationEventCounter;
  private final Terminable owningIterativeTask;

  private static final Log log = LogFactory.getLog(InterruptingMutableObjectIterator.class);

  public InterruptingMutableObjectIterator(MutableObjectIterator<E> delegate, int numberOfEventsUntilInterrupt,
      String name, Terminable owningIterativeTask) {
    Preconditions.checkArgument(numberOfEventsUntilInterrupt > 0);
    this.delegate = delegate;
    this.numberOfEventsUntilInterrupt = numberOfEventsUntilInterrupt;
    this.name = name;
    this.owningIterativeTask = owningIterativeTask;

    endOfSuperstepEventCounter = new AtomicInteger(0);
    terminationEventCounter = new AtomicInteger(0);
  }

  @Override
  public void eventOccurred(AbstractTaskEvent event) {

    if (EndOfSuperstepEvent.class.equals(event.getClass())) {
      onEndOfSuperstep();
      return;
    }

    if (TerminationEvent.class.equals(event.getClass()))   {
      onTermination();
      return;
    }

    throw new IllegalStateException("Unable to handle event " + event.getClass().getName());
  }

  private void onTermination() {
    int numberOfEventsSeen = terminationEventCounter.incrementAndGet();
    if (log.isInfoEnabled()) {
      log.info("InterruptibleIterator of " + name + " received Termination event (" + numberOfEventsSeen +")");
    }

    Preconditions.checkState(numberOfEventsSeen <= numberOfEventsUntilInterrupt);

    if (numberOfEventsSeen == numberOfEventsUntilInterrupt) {
      owningIterativeTask.requestTermination();
    }
  }

  private void onEndOfSuperstep() {
    int numberOfEventsSeen = endOfSuperstepEventCounter.incrementAndGet();
    if (log.isInfoEnabled()) {
      log.info("InterruptibleIterator of " + name + " received EndOfSuperstep event (" + numberOfEventsSeen +")");
    }

    if (numberOfEventsSeen % numberOfEventsUntilInterrupt == 0) {
      Thread.currentThread().interrupt();
    }
  }

//  private int recordsRead = 0;

  @Override
  public boolean next(E target) throws IOException {

//    log.info("InterruptibleIterator of " + name + " waiting for record("+ (recordsRead) +")");

    boolean recordFound = delegate.next(target);

//    if (recordFound) {
//      log.info("InterruptibleIterator of " + name + " read record("+ (recordsRead) +")");
//      recordsRead++;
//    } else {

    if (!recordFound && log.isInfoEnabled()) {
      log.info("InterruptibleIterator of " + name + " releases input");
    }
    return recordFound;
  }

}
