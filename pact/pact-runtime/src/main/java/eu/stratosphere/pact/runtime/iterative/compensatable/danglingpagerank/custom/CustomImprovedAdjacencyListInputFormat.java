package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom;

import eu.stratosphere.pact.generic.io.DelimitedInputFormat;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.AsciiLongArrayView;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithAdjacencyList;

public class CustomImprovedAdjacencyListInputFormat extends DelimitedInputFormat<VertexWithAdjacencyList> {

	private final AsciiLongArrayView arrayView = new AsciiLongArrayView();

	@Override
	public boolean readRecord(VertexWithAdjacencyList target, byte[] bytes, int offset, int numBytes) {

		if (numBytes == 0) {
			return false;
		}

		arrayView.set(bytes, offset, numBytes);
		
		long[] list = target.getTargets();

		try {

			int pos = 0;
			while (arrayView.next()) {

				if (pos == 0) {
					target.setVertexID(arrayView.element());
				} else {
					if (list.length <= pos - 1) {
						list = new long[list.length < 16 ? 16 : list.length * 2];
						target.setTargets(list);
					}
					list[pos - 1] = arrayView.element();
				}
				pos++;
			}
			
			target.setNumTargets(pos - 1);
		} catch (RuntimeException e) {
			throw new RuntimeException("Error parsing: " + arrayView.toString(), e);
		}

		return true;
	}
}
