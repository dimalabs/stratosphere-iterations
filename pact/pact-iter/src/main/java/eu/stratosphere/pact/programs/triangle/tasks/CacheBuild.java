package eu.stratosphere.pact.programs.triangle.tasks;

import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.ParallelTriangleEntry;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore.CacheType;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class CacheBuild extends AbstractMinimalTask {
	
	public static final String CACHE_ID_PARAM = "iter.cache.cacheid";
	
	public static final String CACHE_TYPE_PARAM = "iter.cache.cachetype";
	
	private String cacheId;
	
	private CacheType cacheType;
	

	@Override
	public void initTask() {		
		//Read cachedId from config
		cacheId = config.getStubParameter(CACHE_ID_PARAM, null);
		if(cacheId == null) {
			throw new RuntimeException("Cache ID missing");
		}
		
		//Read cacheType from config
		cacheType = CacheType.valueOf(config.getStubParameter(CACHE_TYPE_PARAM, null));
	}

	@Override
	public void invoke() throws Exception {
		//Create new cache with the base name + task id as combined identifier
		int subTaskIndex = this.getEnvironment().getIndexInSubtaskGroup();
		CacheStore.createCache(cacheId, subTaskIndex, cacheType, Integer.class, Integer.class);
		ConcurrentMap<Integer, ParallelTriangleEntry> cache = 
				CacheStore.getInsertCache(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);

		MutableObjectIterator<PactRecord> input = inputs[0];
		PactRecord rec = new PactRecord();
		while(input.next(rec)) {
			Integer nodeId = rec.getField(0, PactInteger.class).getValue();
			Integer neighbourId = rec.getField(1, PactInteger.class).getValue();
			
			ParallelTriangleEntry entry = cache.get(nodeId);
			if (entry == null) {
				entry = new ParallelTriangleEntry();
				ParallelTriangleEntry old = cache.putIfAbsent(nodeId, entry);
				entry = old == null ? entry : old;
			}
			entry.add(neighbourId);
		}
		
		output.close();
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}
}
