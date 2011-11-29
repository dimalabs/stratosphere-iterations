package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.core.AbstractMinimalTask;

public class KtrussFiltering extends AbstractMinimalTask {
	
	public static final String KTRUSS_K_VALUE = "ktrusses.k";
	
	private int k = -1;
	
	@Override
	protected void initTask() {
		//Read cachedId from config
		String kValue = config.getStubParameter(KTRUSS_K_VALUE, null);
		if(kValue == null) {
			throw new RuntimeException("Ktrusses k missing");
		}
		
		k = Integer.valueOf(k);
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void invoke() throws Exception {
		// TODO Auto-generated method stub

	}

}
