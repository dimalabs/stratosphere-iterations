/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.generic.GenericCrosser;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;


public class CrossTaskExternalITCase extends DriverTestBase<GenericCrosser<PactRecord, PactRecord, PactRecord>>
{
	private static final Log LOG = LogFactory.getLog(CrossTaskExternalITCase.class);
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();

	
	public CrossTaskExternalITCase() {
		super(1*1024*1024);
	}
	
	@Test
	public void testExternalBlockCrossTask() {

		int keyCnt1 = 2;
		int valCnt1 = 1;
		
		// 43690 fit into memory, 43691 do not!
		int keyCnt2 = 43700;
		int valCnt2 = 1;
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addOutput(this.outList);
		
		CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
		super.getTaskConfig().setMemorySize(1 * 1024 * 1024);
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testExternalStreamCrossTask() {

		int keyCnt1 = 2;
		int valCnt1 = 1;
		
		// 87381 fit into memory, 87382 do not!
		int keyCnt2 = 87385;
		int valCnt2 = 1;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		super.addOutput(this.outList);
		
		CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		super.getTaskConfig().setMemorySize(1 * 1024 * 1024);
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	public static class MockCrossStub extends CrossStub {

		@Override
		public void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
			
			out.collect(record1);
			
		}
	}
	
}
