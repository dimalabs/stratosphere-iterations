package eu.stratosphere.nephele.jobmanager;


import java.util.Iterator;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.io.BroadcastRecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.nephele.types.StringRecord;

public class BroadcastSourceTask extends AbstractFileInputTask {

	/**
	 * The broadcast record writer the records will be emitted through.
	 */
	private BroadcastRecordWriter<StringRecord> output;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.output = new BroadcastRecordWriter<StringRecord>(this, StringRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		final Iterator<FileInputSplit> splitIterator = getFileInputSplits();

		while (splitIterator.hasNext()) {

			final FileInputSplit split = splitIterator.next();

			final long start = split.getStart();
			final long length = split.getLength();

			final FileSystem fs = FileSystem.get(split.getPath().toUri());

			final FSDataInputStream fdis = fs.open(split.getPath());

			final LineReader lineReader = new LineReader(fdis, start, length, (1024 * 1024));

			byte[] line = lineReader.readLine();

			while (line != null) {

				// Create a string object from the data read
				StringRecord str = new StringRecord();
				str.set(line);
				// Send out string
				output.emit(str);

				line = lineReader.readLine();
			}

			// Close the stream;
			lineReader.close();
		}
		
	}

}
