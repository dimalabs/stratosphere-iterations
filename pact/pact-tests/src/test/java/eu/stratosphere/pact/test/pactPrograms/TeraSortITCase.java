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

package eu.stratosphere.pact.test.pactPrograms;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.example.terasort.TeraSort;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class TeraSortITCase extends TestBase
{
	private static final String INPUT_DATA_FILE = "/testdata/terainput.txt";
	
	private String resultPath;
	
	public TeraSortITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception
	{
		resultPath = getFilesystemProvider().getTempDirPath() + "/result";
	}

	@Override
	protected JobGraph getJobGraph() throws Exception
	{
		URL fileURL = getClass().getResource(INPUT_DATA_FILE);
		String inPath = "file://" + fileURL.getPath();
			
		TeraSort ts = new TeraSort();
		Plan plan = ts.getPlan(this.config.getString("TeraSortITCase#NoSubtasks", "1"),
			inPath, getFilesystemProvider().getURIPrefix() + resultPath);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception
	{	
		final byte[] line = new byte[100];
		final byte[] previous = new byte[10];
		for (int i = 0; i < previous.length; i++) {
			previous[i] = -128;
		}
		
		File parent = new File(this.resultPath);
		int num = 1;
		while (true) {
			File next = new File(parent, String.valueOf(num));
			if (!next.exists()) {
				break;
			}
			FileInputStream inStream = new FileInputStream(next);
			int read;
			while ((read = inStream.read(line)) == 100) {
				// check against the previous
				for (int i = 0; i < previous.length; i++) {
					if (line[i] > previous[i]) {
						break;
					} else if (line[i] < previous[i]) {
						Assert.fail("Next record is smaller than previous record.");
					}
				}
				
				System.arraycopy(line, 0, previous, 0, 10);
			}
			
			if (read != -1) {
				Assert.fail("Inclomplete last record in result file.");
			}
			inStream.close();
			
			num++;
		}
		
		if (num == 1) {
			Assert.fail("Empty result, nothing checked for Job!");
		}
		
		getFilesystemProvider().delete(resultPath, true);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations()
	{
		final List<Configuration> tConfigs = new ArrayList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("TeraSortITCase#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}
}
