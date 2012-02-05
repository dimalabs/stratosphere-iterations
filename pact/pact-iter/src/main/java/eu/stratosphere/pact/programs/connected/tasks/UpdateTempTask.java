package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.TempTaskV2;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;

public class UpdateTempTask extends TempTaskV2 {
	
	protected void initTask() {
		accessors[0] = new ComponentUpdateAccessor();
		outputAccessors[0] = new ComponentUpdateAccessor();
	}
	
}
