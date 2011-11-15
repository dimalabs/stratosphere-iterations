package eu.stratosphere.pact.iterative.nephele.tasks.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

public class ProgressMonitor {
	
	public static enum Progress_State {OPEN, CLOSED};
	
	protected MutableObjectIterator<PactRecord> progressInput;
	
	protected volatile Progress_State currentState = Progress_State.CLOSED;
	
	public ProgressMonitor(MutableObjectIterator<PactRecord> input, String name) {
		Thread t = new Thread(new ProgressUpdate());
		t.setDaemon(true);
		t.setName("Progress Monitor: " + name);
		t.start();
		//TODO: Stop thread
	}
	
	public Progress_State getCurrentState() {
		return currentState;
	}
	
	private class ProgressUpdate implements Runnable {
		
		@Override
		public void run() {
			PactRecord progressUpdate = new PactRecord();
			
			try {
				while(progressInput.next(progressUpdate)) {
					currentState = progressUpdate.getField(0, StateValue.class).getValue();
				}
			} catch (IOException e) {
				throw new RuntimeException();
			}
		}
		
	}

	public static class StateValue implements Value {
		
		Progress_State state;
		
		public StateValue(Progress_State state) {
			this.state = state;
		}

		public Progress_State getValue() {
			return state;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(state.name());
		}

		@Override
		public void read(DataInput in) throws IOException {
			state = Progress_State.valueOf(in.readUTF());
		}
		
	}
}
