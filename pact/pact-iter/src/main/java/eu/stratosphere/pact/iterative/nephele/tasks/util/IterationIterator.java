package eu.stratosphere.pact.iterative.nephele.tasks.util;

import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.util.ChannelStateEvent.ChannelState;

public class IterationIterator implements MutableObjectIterator<PactRecord> {
	private MutableObjectIterator<PactRecord> iter;
	private PactRecord firstRecord;
	private ChannelStateTracker listener;
	private boolean closed;
	
	public IterationIterator(PactRecord rec,
			MutableObjectIterator<PactRecord> input, ChannelStateTracker listener) {
		this.iter = input;
		this.firstRecord = rec;
		this.listener = listener;
	}

	@Override
	public boolean next(PactRecord target) throws IOException {
		if(firstRecord != null) {
			firstRecord.copyTo(target);
			firstRecord = null;
			return true;
		}
		
		if(closed) {
			return false;
		}
		
		//Needs to be in a loop, because multiple events can come between records
		//(every event needs one loop because of the exceptions)
		while(true) {
			try {
				boolean success = iter.next(target);
				
				if(!success) {
					//Channel should be closed before it is exhausted
					throw new RuntimeException("Channel is not closed but exhausted");
				}
				return success;
			} catch (StateChangeException ex) {
				//Only valid state changed is from open to closed
				if(listener.isChanged()) {
					ChannelState state = listener.getState();
					if(state == ChannelState.CLOSED) {
						closed = true;
						return false;
					} else {
						throw new RuntimeException("Should never happen");
					}
				}
			}
		}
	}
}
