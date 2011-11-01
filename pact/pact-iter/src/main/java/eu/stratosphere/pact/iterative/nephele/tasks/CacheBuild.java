package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.ParallelTriangleEntry;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore;

public class CacheBuild extends AbstractMinimalTask {
	private static final Log LOG = LogFactory.getLog(CacheBuild.class);
	
	public static final String CACHE_ID_PARAM = "iter.cache.cacheid";
	
	private String cacheId;
	

	@Override
	public void initTask() {		
		//Read cachedId from config
		cacheId = config.getStubParameter(CACHE_ID_PARAM, null);
		if(cacheId == null) {
			throw new RuntimeException("Cache ID missing");
		}
	}

	@Override
	public void invoke() throws Exception {
		LOG.warn("Invoked cache build");
		//Create new cache with the base name + task id as combined identifier
		String taskCacheId = cacheId + this.getEnvironment().getIndexInSubtaskGroup();
		CacheStore.createCache(taskCacheId, Integer.class, Integer.class);
		ConcurrentMap<Integer, ParallelTriangleEntry> cache = 
				CacheStore.getCache(taskCacheId, Integer.class, ParallelTriangleEntry.class);

		MutableObjectIterator<PactRecord> input = inputs[0];
		PactRecord rec = new PactRecord();
		while(input.next(rec)) {LOG.warn("Record");
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
