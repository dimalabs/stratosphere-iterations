package eu.stratosphere.pact.iterative.nephele.tasks.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.pact.common.type.PactRecord;

public class PactRecordEvent extends AbstractTaskEvent {

	private PactRecord rec;

	public PactRecordEvent(PactRecord rec) {
		this.rec = rec;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		rec.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		rec = new PactRecord();
		rec.read(in);
	}

	public PactRecord getRecord() {
		return rec;
	}

}
