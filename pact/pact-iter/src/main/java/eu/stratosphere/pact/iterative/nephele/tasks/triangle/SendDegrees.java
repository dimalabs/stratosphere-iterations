package eu.stratosphere.pact.iterative.nephele.tasks.triangle;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.iterative.ParallelTriangleEntry;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore;
import eu.stratosphere.pact.iterative.nephele.tasks.core.AbstractMinimalTask;

public class SendDegrees extends AbstractMinimalTask {

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
		
		int subTaskIndex = this.getEnvironment().getIndexInSubtaskGroup();
		ConcurrentMap<Integer, ParallelTriangleEntry> cache = 
				CacheStore.getLookupCache(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);
		
		PactRecord out = new PactRecord();
		Iterator<Entry<Integer, ParallelTriangleEntry>> iterator = 
				CacheStore.getCachePartition(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);
		
		while (iterator.hasNext()) {
			final Map.Entry<Integer, ParallelTriangleEntry> entry = iterator.next();
			final int key = entry.getKey().intValue();
			final ParallelTriangleEntry tEntry = entry.getValue();
			final int degree = tEntry.size();
			
			for (int i = 0; i < degree; i++) {
				Integer id = Integer.valueOf(tEntry.getId(i));
				
				// tell that id about this key's degree
				ParallelTriangleEntry other = cache.get(id);
				if(other != null) {
					other.setDegree(key, degree);
				} else {
					out.setField(0, new PactInteger(id));
					out.setField(1, new PactInteger(key));
					out.setField(2, new PactInteger(degree));
					output.collect(out);
				}
			}
		}
		
		output.close();
	}

}
