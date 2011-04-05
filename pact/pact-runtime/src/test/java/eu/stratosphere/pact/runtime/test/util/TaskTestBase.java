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

package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.DataSinkTask.DataSinkConfig;
import eu.stratosphere.pact.runtime.task.DataSourceTask.DataSourceConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

@RunWith( PowerMockRunner.class )
@PrepareForTest( LibraryCacheManager.class )
@PowerMockIgnore("org.apache.log4j.*")
public abstract class TaskTestBase {

	long memorySize = 0;
	MockEnvironment mockEnv;
	
	public void initEnvironment(long memorySize) {

		this.memorySize = memorySize;
		this.mockEnv = new MockEnvironment(this.memorySize);
		
		PowerMockito.mockStatic(LibraryCacheManager.class);
        try {
			Mockito.when(LibraryCacheManager.getClassLoader(null)).thenReturn(Thread.currentThread().getContextClassLoader());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void addInput(Iterator<KeyValuePair<PactInteger,PactInteger>> input) {
		this.mockEnv.addInput(input);
		new TaskConfig(mockEnv.getRuntimeConfiguration()).addInputShipStrategy(ShipStrategy.FORWARD);
	}
	
	public void addOutput(List<KeyValuePair<PactInteger,PactInteger>> output) {
		this.mockEnv.addOutput(output);
		new TaskConfig(mockEnv.getRuntimeConfiguration()).addOutputShipStrategy(ShipStrategy.FORWARD);
	}

	public TaskConfig getTaskConfig() {
		return new TaskConfig(mockEnv.getRuntimeConfiguration());
	}
	
	public void registerTask(AbstractTask task, Class<? extends Stub<PactInteger,PactInteger>> stubClass) {
		new TaskConfig(mockEnv.getRuntimeConfiguration()).setStubClass(stubClass);
		task.setEnvironment(mockEnv);
		task.registerInputOutput();
	}
	
	public void registerTask(AbstractTask task) {
		task.setEnvironment(mockEnv);
		task.registerInputOutput();
	}
	
	public void registerTask(AbstractOutputTask outTask, Class<? extends OutputFormat<PactInteger, PactInteger>> stubClass, String outPath) {
		new DataSinkConfig(mockEnv.getRuntimeConfiguration()).setStubClass(stubClass);
		new DataSinkConfig(mockEnv.getRuntimeConfiguration()).setFilePath(outPath);
		outTask.setEnvironment(mockEnv);
		outTask.registerInputOutput();
	}
	
	public void registerTask(AbstractInputTask inTask, Class<? extends InputFormat<PactInteger, PactInteger>> stubClass, String inPath, String delimiter) {
		new DataSourceConfig(mockEnv.getRuntimeConfiguration()).setStubClass(stubClass);
		new DataSourceConfig(mockEnv.getRuntimeConfiguration()).setFilePath(inPath);
		new DataSourceConfig(mockEnv.getRuntimeConfiguration()).getFormatParameters().setString("delimiter", delimiter);
		
		mockEnv.setInputSplits(inPath, 5);
		
		inTask.setEnvironment(mockEnv);
		inTask.registerInputOutput();
	}
	
	public MemoryManager getMemoryManager() {
		return mockEnv.getMemoryManager();
	}
	
	@After
	public void shutdownIOManager() throws Exception {
		mockEnv.getIOManager().shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", mockEnv.getIOManager().isProperlyShutDown());
	}
	
	@After
	public void shutdownMemoryManager() throws Exception {
		if (this.memorySize > 0) {
			MemoryManager memMan = getMemoryManager();
			if (memMan != null) {
				Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
				memMan.shutdown();
			}
		}
	}
	
}