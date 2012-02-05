package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.TempTaskV2;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;

public class UpdateTempTask extends TempTaskV2 {
	
	@Override
	public void prepare() throws Exception {
		accessors[0] = new ComponentUpdateAccessor();
		outputAccessors[0] = new ComponentUpdateAccessor();
		
		super.prepare();
	}
}
