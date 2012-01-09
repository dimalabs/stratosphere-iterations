package eu.stratosphere.pact.programs.triangle.tasks;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore;
import eu.stratosphere.pact.iterative.nephele.io.IntegerHashPartitioner;
import eu.stratosphere.pact.iterative.nephele.io.PactIntArray;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.standalone.ParallelTriangleEntry;

public class SendTrianglesHashPartitioned extends AbstractMinimalTask {

	public static final String CACHE_ID_PARAM = "iter.cache.cacheid";
	
	private String cacheId;

	@Override
	protected void initTask() {
		//Read cachedId from config
		cacheId = config.getStubParameter(CACHE_ID_PARAM, null);
		if(cacheId == null) {
			throw new RuntimeException("Cache ID missing");
		}
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		waitForPreviousTask(inputs[0]);
		
		//Fix output emitter to use simple hash partitioning
		OutputEmitter oe = 
				(OutputEmitter) output.getWriters().get(0).getOutputGate().getChannelSelector();
		oe.setPartitionFunction(new IntegerHashPartitioner());
		
		int subTaskIndex = this.getEnvironment().getIndexInSubtaskGroup();
		ConcurrentMap<Integer, ParallelTriangleEntry> cache = 
				CacheStore.getLookupCache(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);
		
		PactRecord out = new PactRecord();
		Iterator<Entry<Integer, ParallelTriangleEntry>> iterator = 
				CacheStore.getCachePartition(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);
		
		while (iterator.hasNext()) {
			final Map.Entry<Integer, ParallelTriangleEntry> kv = iterator.next();
			final ParallelTriangleEntry entry = kv.getValue();
			final int key = kv.getKey().intValue();
			final int degree = entry.size();
			
			// notify all that have a larger degree of our neighbors with a lower degree than them
			for (int i = 0; i < degree; i++) {
				final int toNotifyId = entry.getId(i);
				final int toNotifyDegree = entry.getDegree(i);
				
				// rule out which ones not to notify
				if (toNotifyDegree < degree || (toNotifyDegree == degree && toNotifyId > key))
					continue;
				
				// tell it all our neighbors with a smaller id than that one itself has
				final ParallelTriangleEntry toNotify = cache.get(Integer.valueOf(toNotifyId));
				if(toNotify != null) {
					toNotify.addTriangles(entry.getAllIds(), i, key, toNotifyId, null);
				} else {
					out.setField(0, new PactInteger(toNotifyId));
					out.setField(1, new PactInteger(i));
					out.setField(2, new PactInteger(key));
					out.setField(3, new PactIntArray(entry.getAllIds()));
					output.collect(out);
				}
			}
		}
		
		output.close();
	}

}
