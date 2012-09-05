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

//package eu.stratosphere.pact.compiler;
//
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//
//import junit.framework.Assert;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import eu.stratosphere.nephele.instance.HardwareDescription;
//import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
//import eu.stratosphere.nephele.instance.InstanceType;
//import eu.stratosphere.nephele.instance.InstanceTypeDescription;
//import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
//import eu.stratosphere.nephele.instance.InstanceTypeFactory;
//import eu.stratosphere.pact.common.contract.FileDataSinkContract;
//import eu.stratosphere.pact.common.contract.FileDataSourceContract;
//import eu.stratosphere.pact.common.contract.MapContract;
//import eu.stratosphere.pact.common.contract.ReduceContract;
//import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
//import eu.stratosphere.pact.common.io.TextInputFormat;
//import eu.stratosphere.pact.common.plan.Plan;
//import eu.stratosphere.pact.common.plan.Visitor;
//import eu.stratosphere.pact.common.type.base.PactInteger;
//import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
//import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
//import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
//import eu.stratosphere.pact.compiler.plan.OptimizerNode;
//import eu.stratosphere.pact.compiler.plan.ReduceNode;
//import eu.stratosphere.pact.compiler.util.DummyInputFormat;
//import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
//import eu.stratosphere.pact.compiler.util.IdentityMap;
//import eu.stratosphere.pact.compiler.util.IdentityReduce;
//import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
//
///**
// * Tests in this class:
// * <ul>
// *   <li> Tests if PARTITION_LOCAL_HASH strategy is chosen when data already has an partitioning and dop is increasing
// *   <li> Tests that no error is thrown in the JobGraphGenerator for PARTITION_LOCAL_HASH (ticket #170)
// * </ul>
// *
// * @author Moritz Kaufmann
// */
//public class PartitionLocalHashCompilerTest {
//	private static final String IN_FILE_1 = "file:///test/file";
//	
//	private static final String OUT_FILE_1 = "file///test/output";
//	
//	private static final int defaultParallelism = 8;
//	
//	// ------------------------------------------------------------------------
//	
//	private PactCompiler compiler;
//	
//	private InstanceTypeDescription instanceType;
//	
//	// ------------------------------------------------------------------------	
//	
//	@Before
//	public void setup()
//	{
//		try {
//			InetSocketAddress dummyAddress = new InetSocketAddress(InetAddress.getLocalHost(), 12345);
//			
//			// prepare the statistics
//			DataStatistics dataStats = new DataStatistics();
//			dataStats.cacheBaseStatistics(new TextInputFormat.FileBaseStatistics(1000, 128 * 1024 * 1024, 8.0f),
//				FileDataSourceContract.getInputIdentifier(DummyInputFormat.class, IN_FILE_1));
//			
//			this.compiler = new PactCompiler(dataStats, new FixedSizeClusterCostEstimator(), dummyAddress);
//		}
//		catch (Exception ex) {
//			ex.printStackTrace();
//			Assert.fail("Test setup failed.");
//		}
//		
//		// create the instance type description
//		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
//		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
//		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, defaultParallelism * 2);
//	}
//	
//	@Test
//	public void testPartitionLocalHashChosen() {
//		// construct the plan
//		FileDataSourceContract<PactInteger, PactInteger> source = new FileDataSourceContract<PactInteger, PactInteger>(DummyInputFormat.class, IN_FILE_1, "Source");
//		source.setDegreeOfParallelism(defaultParallelism);
//		source.setOutputContract(UniqueKey.class);
//		
//		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map1 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map1");
//		map1.setDegreeOfParallelism(1);
//		map1.setInput(source);
//		
//		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce1 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce1");
//		reduce1.setDegreeOfParallelism(defaultParallelism);
//		reduce1.setInput(map1);
//		
//		FileDataSinkContract<PactInteger, PactInteger> sink = new FileDataSinkContract<PactInteger, PactInteger>(DummyOutputFormat.class, OUT_FILE_1, "Sink");
//		sink.setDegreeOfParallelism(defaultParallelism);
//		sink.setInput(reduce1);
//		
//		Plan plan = new Plan(sink, "Test Temp Task");
//		
//		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
//		//Inject temp strategy
//		oPlan.accept(new Visitor<OptimizerNode>() {
//			@Override
//			public boolean preVisit(OptimizerNode visitable) {
//				if(visitable instanceof ReduceNode) {
//					//Check that correct strategy is chosen
//					ShipStrategy strategy = visitable.getIncomingConnections().get(0).getShipStrategy();
//					Assert.assertEquals(ShipStrategy.PARTITION_LOCAL_HASH, strategy);
//					return false;
//				}
//				return true;
//			}
//			
//			@Override
//			public void postVisit(OptimizerNode visitable) {
//			}
//		});
//		
//		JobGraphGenerator jobGen = new JobGraphGenerator();
//		
//		//Compile plan to verify that no error is thrown
//		jobGen.compileJobGraph(oPlan);
//	}
//}
