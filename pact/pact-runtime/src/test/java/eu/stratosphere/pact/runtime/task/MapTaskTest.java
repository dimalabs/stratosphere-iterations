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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.test.util.InfiniteInputIterator;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

@SuppressWarnings("javadoc")
public class MapTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(MapTaskTest.class);
	
	List<PactRecord> outList;
		
	@Test
	public void testMapTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		this.outList = new ArrayList<PactRecord>();
		
		super.initEnvironment(1);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false), 1);
		super.addOutput(this.outList);
		
		MapTask testTask = new MapTask();
		
		super.registerTask(testTask, MockMapStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue(this.outList.size() == keyCnt*valCnt);
		
	}
	
	@Test
	public void testFailingMapTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		this.outList = new ArrayList<PactRecord>();
		
		super.initEnvironment(1);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false), 1);
		super.addOutput(this.outList);
		
		MapTask testTask = new MapTask();
		
		super.registerTask(testTask, MockFailingMapStub.class);
		
		boolean stubFailed = false;
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			stubFailed = true;
		}
		
		Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
		
	}
	
	@Test
	public void testCancelMapTask() {
		
		super.initEnvironment(1);
		super.addInput(new InfiniteInputIterator(), 1);
		super.addOutput(new NirvanaOutputList());
		
		final MapTask testTask = new MapTask();
		
		super.registerTask(testTask, MockMapStub.class);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testTask.invoke();
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
				}				
			}
		};
		taskRunner.start();
		
		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, testTask);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
				
	}
	
	public static class MockMapStub extends MapStub {

		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			out.collect(record);
		}
		
	}
	
	public static class MockFailingMapStub extends MapStub {

		int cnt = 0;
		
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			if(++this.cnt>=10) {
				throw new RuntimeException("Expected Test Exception");
			}
			out.collect(record);
		}
		
	}
	
}
