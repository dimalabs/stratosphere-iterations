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

package eu.stratosphere.pact.test.contracts;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.contracts.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.pact.test.contracts.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.pact.test.util.TestBase;

/**
 * @author Matthias Ringwald
 */
@RunWith(Parameterized.class)
public class UnionITCase extends TestBase

{
	private static final Log LOG = LogFactory.getLog(UnionITCase.class);
	
	public UnionITCase(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String MAP_IN_1 = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_IN_2 = "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n";

	private static final String MAP_IN_3 = "1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n";

	private static final String MAP_IN_4 = "1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	private static final String EMPTY_MAP_RESULT = "";
	
	private static final String MAP_RESULT_TWICE = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n" +
												"1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";
	
	private static final String emptyInputFilePathPostfix = "/emptyInput";
	
	private static final String inputFilePathPostfix = "/mapInput";
	
	@Override
	protected void preSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();
		
		getFilesystemProvider().createDir(tempDir + inputFilePathPostfix);
		
		getFilesystemProvider().createFile(tempDir+inputFilePathPostfix+"/UnionTest_1.txt", MAP_IN_1);
		getFilesystemProvider().createFile(tempDir+inputFilePathPostfix+"/UnionTest_2.txt", MAP_IN_2);
		getFilesystemProvider().createFile(tempDir+inputFilePathPostfix+"/UnionTest_3.txt", MAP_IN_3);
		getFilesystemProvider().createFile(tempDir+inputFilePathPostfix+"/UnionTest_4.txt", MAP_IN_4);
		
		getFilesystemProvider().createDir(tempDir + emptyInputFilePathPostfix);
		
		getFilesystemProvider().createFile(tempDir+emptyInputFilePathPostfix+"/UnionTest_1.txt", "");
		getFilesystemProvider().createFile(tempDir+emptyInputFilePathPostfix+"/UnionTest_2.txt", "");
		getFilesystemProvider().createFile(tempDir+emptyInputFilePathPostfix+"/UnionTest_3.txt", "");
		getFilesystemProvider().createFile(tempDir+emptyInputFilePathPostfix+"/UnionTest_4.txt", "");
	}

	public static class TestMapper extends MapStub {

		private PactString keyString = new PactString();
		private PactString valueString = new PactString();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			keyString = record.getField(0, keyString);
			valueString = record.getField(1, valueString);
			
			LOG.debug("Processed: [" + keyString.toString() + "," + valueString.getValue() + "]");
			
			if (Integer.parseInt(keyString.toString()) + Integer.parseInt(valueString.toString()) < 10) {

				record.setField(0, valueString);
				record.setField(1, new PactInteger(Integer.parseInt(keyString.toString()) + 10));
				
				out.collect(record);
			}
			
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		String pathPrefix = getFilesystemProvider().getURIPrefix()+getFilesystemProvider().getTempDirPath();
		
		
		FileDataSource input1 = new FileDataSource(
			ContractITCaseInputFormat.class, pathPrefix + config.getString("UnionTest#Input1Path", ""));
		input1.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		input1.setDegreeOfParallelism(config.getInteger("UnionTest#NoSubtasks", 1));
		
		FileDataSource input2 = new FileDataSource(
				ContractITCaseInputFormat.class, pathPrefix + config.getString("UnionTest#Input2Path", ""));
		input2.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		input2.setDegreeOfParallelism(config.getInteger("UnionTest#NoSubtasks", 1));
		
		MapContract testMapper = new MapContract(TestMapper.class);
		testMapper.setDegreeOfParallelism(config.getInteger("UnionTest#NoSubtasks", 1));

		FileDataSink output = new FileDataSink(
				ContractITCaseOutputFormat.class, pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.addInput(testMapper);

		testMapper.addInput(input1);
		testMapper.addInput(input2);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();
		
		compareResultsByLinesInMemory(config.getString("UnionTest#ExpectedResult", ""), tempDir+ "/result.txt");
		
		getFilesystemProvider().delete(tempDir+ "/result.txt", true);
		getFilesystemProvider().delete(tempDir+ inputFilePathPostfix, true);
		getFilesystemProvider().delete(tempDir+ emptyInputFilePathPostfix, true);
		
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		//second input empty
		Configuration config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", MAP_RESULT);
		config.setString("UnionTest#Input1Path", inputFilePathPostfix);
		config.setString("UnionTest#Input2Path", emptyInputFilePathPostfix);
		testConfigs.add(config);
		
		
		//first input empty
		config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", MAP_RESULT);
		config.setString("UnionTest#Input1Path", emptyInputFilePathPostfix);
		config.setString("UnionTest#Input2Path", inputFilePathPostfix);
		testConfigs.add(config);
		
		//both inputs full
		config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", MAP_RESULT_TWICE);
		config.setString("UnionTest#Input1Path", inputFilePathPostfix);
		config.setString("UnionTest#Input2Path", inputFilePathPostfix);
		testConfigs.add(config);
		
		//both inputs empty
		config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", EMPTY_MAP_RESULT);
		config.setString("UnionTest#Input1Path", emptyInputFilePathPostfix);
		config.setString("UnionTest#Input2Path", emptyInputFilePathPostfix);
		testConfigs.add(config);

		return toParameterList(UnionITCase.class, testConfigs);
	}
}
