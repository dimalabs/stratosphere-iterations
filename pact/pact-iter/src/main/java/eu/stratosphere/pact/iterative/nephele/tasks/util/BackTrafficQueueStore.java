package eu.stratosphere.pact.iterative.nephele.tasks.util;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateEvent.ChannelState;

public class BackTrafficQueueStore {

	private volatile HashMap<String, Queue<PactRecord>> queueMap =
			new HashMap<String, Queue<PactRecord>>();
	private volatile HashMap<String, BlockingQueue<ChannelState>> blockingMap =
			new HashMap<String, BlockingQueue<ChannelState>>();
	
	private static BackTrafficQueueStore store = new BackTrafficQueueStore();
	
	public static BackTrafficQueueStore getInstance() {
		return store;
	}

	public synchronized void addStructures(JobID jobID, int subTaskId) {
		if(queueMap.containsKey(getIdentifier(jobID, subTaskId))) {
			throw new RuntimeException("Internal Error");
		}
		
		BlockingQueue<ChannelState> blockingQueue = new ArrayBlockingQueue<ChannelState>(1);
		Queue<PactRecord> queue = new ArrayDeque<PactRecord>(100);
		
		queueMap.put(getIdentifier(jobID, subTaskId), queue);
		blockingMap.put(getIdentifier(jobID, subTaskId), blockingQueue);
	}
	
	public synchronized Queue<PactRecord> retrieveAndReplaceRecordQueue(JobID jobID, int subTaskId) {
		Queue<PactRecord> queue = queueMap.get(getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		//Use new queue for next run, so that records don't mix
		queueMap.put(getIdentifier(jobID, subTaskId), new ArrayDeque<PactRecord>(queue.size()+1));
		return queue;
	}
	
	public synchronized Queue<PactRecord> getRecordQueue(JobID jobID, int subTaskId) {
		Queue<PactRecord> queue = queueMap.get(getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		return queue;
	}
	
	public void waitForIterationEnd(JobID jobID, int subTaskId) throws InterruptedException {
		BlockingQueue<ChannelState> queue = blockingMap.get(getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		queue.take();
	}
	
	public void sendEnd(JobID jobID, int subTaskId) {
		BlockingQueue<ChannelState> queue = blockingMap.get(getIdentifier(jobID, subTaskId));
		if(queue == null) {
			throw new RuntimeException("Internal Error");
		}
		
		queue.add(ChannelState.CLOSED);
	}
	
	private String getIdentifier(JobID jobID, int subTaskId) {
		return jobID.toString() + "#" + subTaskId;
	}
}
