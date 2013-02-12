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

package eu.stratosphere.pact.runtime.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.stub.GenericMatcher;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.sort.MergeMatchIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * Match task which is executed by a Nephele task manager. The task has two inputs and one or multiple outputs.
 * It is provided with a MatchStub implementation.
 * <p>
 * The MatchTask matches all pairs of records that share the same key and come from different inputs. Each pair of 
 * matching records is handed to the <code>match()</code> method of the MatchStub.
 * 
 * @see MatchStub
 */
public class MatchDriver<IT1, IT2, OT> implements PactDriver<GenericMatcher<IT1, IT2, OT>, OT>
{
	protected static final Log LOG = LogFactory.getLog(MatchDriver.class);
	
	protected PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> taskContext;
	
	private volatile MatchTaskIterator<IT1, IT2, OT> matchIterator;		// the iterator that does the actual matching
	
	protected volatile boolean running;
	
	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#setup(eu.stratosphere.pact.runtime.task.PactTaskContext)
	 */
	@Override
	public void setup(PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<GenericMatcher<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericMatcher<IT1, IT2, OT>> clazz = (Class<GenericMatcher<IT1, IT2, OT>>) (Class<?>) GenericMatcher.class;
		return clazz;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
	 */
	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		final TaskConfig config = this.taskContext.getTaskConfig();
		
		// obtain task manager's memory manager and I/O manager
		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		final IOManager ioManager = this.taskContext.getIOManager();
		
		// set up memory and I/O parameters
		final long availableMemory = config.getMemoryDriver();
		final int numPages = memoryManager.computeNumberOfPages(availableMemory);
		
		// test minimum memory requirements
		final DriverStrategy ls = config.getDriverStrategy();
		
		final MutableObjectIterator<IT1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<IT2> in2 = this.taskContext.getInput(1);
		
		// get the key positions and types
		final TypeSerializer<IT1> serializer1 = this.taskContext.getInputSerializer(0);
		final TypeSerializer<IT2> serializer2 = this.taskContext.getInputSerializer(1);
		final TypeComparator<IT1> comparator1 = this.taskContext.getInputComparator(0);
		final TypeComparator<IT2> comparator2 = this.taskContext.getInputComparator(1);
		
		final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = config.getPairComparatorFactory(
				this.taskContext.getUserCodeClassLoader());
		if (pairComparatorFactory == null) {
			throw new Exception("Missing pair comparator factory for Match driver");
		}

		// create and return MatchTaskIterator according to provided local strategy.
		switch (ls) {
		case MERGE:
			this.matchIterator = new MergeMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1,
					serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2),
					memoryManager, ioManager, numPages, this.taskContext.getOwningNepheleTask());
			break;
		case HYBRIDHASH_BUILD_FIRST:
			this.matchIterator = new BuildFirstHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1,
				serializer2, comparator2, pairComparatorFactory.createComparator21(comparator1, comparator2),
				memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), availableMemory);
			break;
		case HYBRIDHASH_BUILD_SECOND:
			this.matchIterator = new BuildSecondHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1,
					serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2),
					memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), availableMemory);
			break;
		default:
			throw new Exception("Unsupported driver strategy for Match driver: " + ls.name());
		}
		
		// open MatchTaskIterator - this triggers the sorting or hash-table building
		// and blocks until the iterator is ready
		this.matchIterator.open();
		
		if (LOG.isDebugEnabled())
			LOG.debug(this.taskContext.formatLogString("Match task iterator ready."));
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception {
		final GenericMatcher<IT1, IT2, OT> matchStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		final MatchTaskIterator<IT1, IT2, OT> matchIterator = this.matchIterator;
		
		while (this.running && matchIterator.callWithNextKey(matchStub, collector));
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		if (this.matchIterator != null) {
			this.matchIterator.close();
			this.matchIterator = null;
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cancel()
	 */
	@Override
	public void cancel() {
		this.running = false;
		if (this.matchIterator != null) {
			this.matchIterator.abort();
		}
	}
}
