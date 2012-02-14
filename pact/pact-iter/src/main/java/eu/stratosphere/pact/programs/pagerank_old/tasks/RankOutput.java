package eu.stratosphere.pact.programs.pagerank_old.tasks;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class RankOutput extends FileOutputFormat {

	@Override
	public void writeRecord(PactRecord record) throws IOException {
		stream.write(record.getField(0, PactString.class).getValue().getBytes());
		stream.write(' ');
		stream.write(record.getField(1, PactDouble.class).toString().getBytes());
		stream.write('\n');
	}

}
