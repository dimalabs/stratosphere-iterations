package eu.stratosphere.pact.programs.triangle.tasks;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.iterative.nephele.cache.CacheStore;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.standalone.ParallelTriangleEntry;

public class CountTriangles extends AbstractMinimalTask {

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
		
		PactRecord out = new PactRecord();
		int numTriangles = 0;
		
		Iterator<Entry<Integer, ParallelTriangleEntry>> iterator = 
				CacheStore.getCachePartition(cacheId, subTaskIndex, Integer.class, ParallelTriangleEntry.class);
		
		while (iterator.hasNext()) {
			final Map.Entry<Integer, ParallelTriangleEntry> kv = iterator.next();
			numTriangles += kv.getValue().getNumTriangles();
		}
		
		out.setField(0, new PactInteger(getIndexInSubtaskGroup()));
		out.setField(1, new PactInteger(numTriangles));
		output.collect(out);
		output.close();
	}

}
