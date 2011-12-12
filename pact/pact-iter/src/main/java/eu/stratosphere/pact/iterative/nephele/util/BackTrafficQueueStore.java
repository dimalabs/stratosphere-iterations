package eu.stratosphere.pact.iterative.nephele.util;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.pact.common.type.PactRecord;

public class BackTrafficQueueStore {
	private volatile HashMap<String, BlockingQueue<Queue<PactRecord>>> iterOpenMap =
			new HashMap<String, BlockingQueue<Queue<PactRecord>>>();
	private volatile HashMap<String, BlockingQueue<Queue<PactRecord>>> iterEndMap =
			new HashMap<String, BlockingQueue<Queue<PactRecord>>>();
	
	private static BackTrafficQueueStore store = new BackTrafficQueueStore();
	
	public static BackTrafficQueueStore getInstance() {
		return store;
	}

	public void addStructures(JobID jobID, int subTaskId) {
		synchronized(iterOpenMap) {
			synchronized(iterEndMap) {
				if(iterOpenMap.containsKey(getIdentifier(jobID, subTaskId))) {
					throw new RuntimeException("Internal Error");
				}
				
				BlockingQueue<Queue<PactRecord>> openQueue = 
						new ArrayBlockingQueue<Queue<PactRecord>>(1);
				BlockingQueue<Queue<PactRecord>> endQueue = 
						new ArrayBlockingQueue<Queue<PactRecord>>(1);
				
				iterOpenMap.put(getIdentifier(jobID, subTaskId), openQueue);
				iterEndMap.put(getIdentifier(jobID, subTaskId), endQueue);
			}
		}
	}
	
	public void publishUpdateQueue(JobID jobID, int subTaskId, int initialSize) {
		BlockingQueue<Queue<PactRecord>> queue = 
				 safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		queue.add(new ArrayDeque<PactRecord>(initialSize));
	}
	
	public synchronized Queue<PactRecord> receiveUpdateQueue(JobID jobID, int subTaskId) throws InterruptedException {
		BlockingQueue<Queue<PactRecord>> queue = 
				 safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		return queue.take();
	}
	
	public void publishIterationEnd(JobID jobID, int subTaskId, Queue<PactRecord> outputQueue) {
		BlockingQueue<Queue<PactRecord>> queue = 
				 safeRetrieval(iterEndMap, getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		queue.add(outputQueue);
	}
	
	public Queue<PactRecord> receiveIterationEnd(JobID jobID, int subTaskId) throws InterruptedException {
		BlockingQueue<Queue<PactRecord>> queue = 
				 safeRetrieval(iterEndMap, getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		return queue.take();
	}
	
	private <K,V> V safeRetrieval(HashMap<K, V> map, K key) {
		synchronized(map) {
			return map.get(key);
		}
	}
	
	private String getIdentifier(JobID jobID, int subTaskId) {
		return jobID.toString() + "#" + subTaskId;
	}
}