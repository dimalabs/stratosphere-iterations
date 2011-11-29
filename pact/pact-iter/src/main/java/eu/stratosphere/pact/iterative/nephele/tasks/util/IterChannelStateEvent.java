package eu.stratosphere.pact.iterative.nephele.tasks.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;

public class IterChannelStateEvent extends AbstractTaskEvent {
	public static enum ChannelState {
		STARTED((byte) 1), OPEN((byte) 2), CLOSED((byte) 3);
		
		final byte id;
		
		ChannelState(byte id) {
			this.id = id;
		}
		
		public byte getStateId() {
			return id;
		}
		
		public static ChannelState getStateById(byte id) {
			for (ChannelState state : ChannelState.values()) {
				if(state.getStateId() == id) {
					return state;
				}
			}
			
			throw new IllegalArgumentException("This id " + id + " has no corresponding channel state");
		}
	}

	private ChannelState state;
	
	public IterChannelStateEvent(ChannelState state) {
		this.state = state;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.write(state.getStateId());
	}

	@Override
	public void read(DataInput in) throws IOException {
		state = ChannelState.getStateById(in.readByte());
	}

	public ChannelState getState() {
		return state;
	}

}
