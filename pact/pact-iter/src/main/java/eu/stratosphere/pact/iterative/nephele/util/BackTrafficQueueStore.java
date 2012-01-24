package eu.stratosphere.pact.iterative.nephele.util;

import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.PactRecord;

public class BackTrafficQueueStore {
	private volatile HashMap<String, BlockingQueue<Queue<PactRecord>>> iterOpenMap =
			new HashMap<String, BlockingQueue<Queue<PactRecord>>>();
	private volatile HashMap<String, BlockingQueue<Queue<PactRecord>>> iterEndMap =
			new HashMap<String, BlockingQueue<Queue<PactRecord>>>();
	
	private static final BackTrafficQueueStore store = new BackTrafficQueueStore();
	
	public static BackTrafficQueueStore getInstance() {
		return store;
	}

	public void addStructures(JobID jobID, int subTaskId, long memorySize) {
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
	
	public void releaseStructures(JobID jobID, int subTaskId) {
		synchronized(iterOpenMap) {
			synchronized(iterEndMap) {
				if(!iterOpenMap.containsKey(getIdentifier(jobID, subTaskId))) {
					throw new RuntimeException("Internal Error");
				}
				
				iterOpenMap.remove(getIdentifier(jobID, subTaskId));
				iterEndMap.remove(getIdentifier(jobID, subTaskId));
			}
		}
	}
	
	public void publishUpdateQueue(JobID jobID, int subTaskId, int initialSize, AbstractTask task) {
		publishUpdateQueue(jobID, subTaskId, initialSize, task, UpdateQueueStrategy.IN_MEMORY_SERIALIZED);
	}
	
	public void publishUpdateQueue(JobID jobID, int subTaskId, int initialSize, AbstractTask task, 
			UpdateQueueStrategy strategy) {
		BlockingQueue<Queue<PactRecord>> queue = 
				 safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		queue.add(strategy.getUpdateQueueFactory().createQueue(jobID, subTaskId, initialSize, task));
	}
	
	public synchronized Queue<PactRecord> receiveUpdateQueue(JobID jobID, int subTaskId) throws InterruptedException {
		BlockingQueue<Queue<PactRecord>> queue = null;
		int count = 0;
		while(queue == null) {
			queue = safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
			if(queue == null) {
				Thread.sleep(1000);
				count++;
				if(count == 10) {
					throw new RuntimeException("Internal Error");
				}
			}
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
	
	private static String getIdentifier(JobID jobID, int subTaskId) {
		return jobID.toString() + "#" + subTaskId;
	}
	
	private interface UpdateQueueFactory {
		public Queue<PactRecord> createQueue(JobID id, int subTaskId, int initialSize, AbstractTask task);
	}
	
	public enum UpdateQueueStrategy {
		IN_MEMORY_OBJECTS(getInstance().new ObjectQueueFactory()),
		IN_MEMORY_SERIALIZED(getInstance().new SerializingQueueFactory());
		
		UpdateQueueFactory factory;
		
		UpdateQueueStrategy(UpdateQueueFactory factory) {
			this.factory = factory;
		}
		
		protected UpdateQueueFactory getUpdateQueueFactory() {
			return factory;
		}
	}
	
	private class ObjectQueueFactory implements UpdateQueueFactory {
		@Override
		@SuppressWarnings("serial")
		public Queue<PactRecord> createQueue(JobID id, int subTaskId, int initialSize, final AbstractTask task) {
			return new ArrayDeque<PactRecord>(initialSize) {
				@Override
				public boolean add(PactRecord rec) {
					PactRecord copy = new PactRecord(rec.getNumFields());
					rec.copyTo(copy);
					
					return super.add(copy);
				}
				
				@Override
				public boolean addAll(Collection<? extends PactRecord> c) {
					throw new UnsupportedOperationException("Not implemented");
				}
			};
		}
	}
	
	private class SerializingQueueFactory implements UpdateQueueFactory {		
		@Override
		public Queue<PactRecord> createQueue(final JobID jobID, final int subTaskId,
				int initialSize, final AbstractTask task) {
			return new SerializingQueue(task);
		}
		
	}
}
