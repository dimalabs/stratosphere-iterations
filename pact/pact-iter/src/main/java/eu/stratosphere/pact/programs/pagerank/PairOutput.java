package eu.stratosphere.pact.programs.pagerank;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class PairOutput extends FileOutputFormat {

	@Override
	public void writeRecord(PactRecord record) throws IOException {
		stream.write(record.getField(0, PactString.class).getValue().getBytes());
		stream.write(' ');
		stream.write(record.getField(1, StringSet.class).toString().getBytes());
		stream.write('\n');
	}

}
