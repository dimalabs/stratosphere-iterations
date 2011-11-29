package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.ParallelTriangleEntry;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore;
import eu.stratosphere.pact.iterative.nephele.tasks.core.AbstractMinimalTask;

public class SetDegrees extends AbstractMinimalTask {

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
		int subTaskIndex = this.getEnvironment().getIndexInSubtaskGroup();
		ConcurrentMap<Integer, ParallelTriangleEntry> cache = null;
		
		MutableObjectIterator<PactRecord> input = inputs[0];
		PactRecord in = new PactRecord();
		while(input.next(in)) {
			if(cache == null) {
				cache = CacheStore.getLookupCache(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);
			}
			
			Integer id = in.getField(0, PactInteger.class).getValue();
			int key = in.getField(1, PactInteger.class).getValue();
			int degree = in.getField(2, PactInteger.class).getValue();
			
			cache.get(id).setDegree(key, degree);
		}
		
		output.close();
	}

}
