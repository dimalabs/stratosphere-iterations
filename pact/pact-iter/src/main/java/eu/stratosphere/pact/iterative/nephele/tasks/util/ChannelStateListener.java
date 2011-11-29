package eu.stratosphere.pact.iterative.nephele.tasks.util;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.iterative.nephele.tasks.util.IterChannelStateEvent.ChannelState;

public class ChannelStateListener implements EventListener {
	private volatile Object monitor = new Integer(0);
	
	private volatile ChannelState state = ChannelState.STARTED;
	
	@Override
	public void eventOccurred(AbstractTaskEvent event) {
		IterChannelStateEvent evt = (IterChannelStateEvent) event;
		synchronized (monitor) {
			state = evt.getState();
			monitor.notifyAll();
		}
	}

	public void waitUntil(ChannelState desiredState) throws InterruptedException {
		synchronized (monitor) {
			while(state != desiredState) {
				waitForStateUpdate();
			}
		}
	}
	
	protected void waitForStateUpdate() throws InterruptedException {
		synchronized (monitor) {
			monitor.wait();
		}
	}

}
