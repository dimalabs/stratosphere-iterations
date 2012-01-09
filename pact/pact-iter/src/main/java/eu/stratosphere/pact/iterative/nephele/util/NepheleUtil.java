package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class NepheleUtil {
	// config dir parameters
	private static final String DEFAULT_CONFIG_DIRECTORY = "/home/mkaufmann/stratosphere-0.2/conf";
	private static final String ENV_CONFIG_DIRECTORY = "NEPHELE_CONF_DIR";
	
	public static void setProperty(JobTaskVertex buildCache,
			String key, String value) {
		TaskConfig taskConfig = new TaskConfig(buildCache.getConfiguration());
		taskConfig.setStubParameter(key, value);
		
	}
	
	public static JobInputVertex createInput(Class<? extends InputFormat<? extends InputSplit>> format, 
			String path, JobGraph graph, int dop, int spi) {
		JobInputVertex vertex = createInput(format, path, graph, dop);
		vertex.setNumberOfSubtasksPerInstance(spi);
		return vertex;
	}

	public static JobInputVertex createInput(Class<? extends InputFormat<? extends InputSplit>> format, 
			String path, JobGraph graph, int dop) {
		JobInputVertex sourceVertex = new JobInputVertex("Input task", graph);
		sourceVertex.setInputClass(DataSourceTask.class);
		sourceVertex.setNumberOfSubtasks(dop);
		
		TaskConfig sourceConfig = new TaskConfig(sourceVertex.getConfiguration());
		sourceConfig.setStubClass(format);
		sourceConfig.setLocalStrategy(LocalStrategy.NONE);
		sourceConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, path);
		
		return sourceVertex;
	}
	
	public static JobOutputVertex createOutput(Class<? extends OutputFormat> format, 
			String path, JobGraph graph, int dop, int spi) {
		JobOutputVertex vertex = createOutput(format, path, graph, dop);
		vertex.setNumberOfSubtasksPerInstance(spi);
		return vertex;
	}
	
	public static JobOutputVertex createOutput(Class<? extends OutputFormat> format, String path,
			JobGraph graph, int dop) {
		JobOutputVertex sinkVertex = new JobOutputVertex("Output task", graph);
		sinkVertex.setOutputClass(DataSinkTask.class);
		sinkVertex.setNumberOfSubtasks(dop);
		sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, dop);
		
		TaskConfig sinkConfig = new TaskConfig(sinkVertex.getConfiguration());
		sinkConfig.setStubClass(format);
		sinkConfig.setLocalStrategy(LocalStrategy.NONE);
		sinkConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, path);
		
		return sinkVertex;
	}
	
	public static JobOutputVertex createDummyOutput(JobGraph graph, int dop, int spi) {
		JobOutputVertex vertex = createDummyOutput(graph, dop);
		vertex.setNumberOfSubtasksPerInstance(spi);
		return vertex;
	}
	
	public static JobOutputVertex createDummyOutput(JobGraph graph, int dop) {
		JobOutputVertex sinkVertex = new JobOutputVertex("Dummy output task", graph);
		sinkVertex.setOutputClass(DummyNullOutput.class);
		sinkVertex.setNumberOfSubtasks(dop);
		sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, dop);
		
		return sinkVertex;
	}
	
	public static JobTaskVertex createTask(Class<? extends AbstractMinimalTask> task, JobGraph graph, int dop, int spi) {
		JobTaskVertex vertex = createTask(task, graph, dop);
		vertex.setNumberOfSubtasksPerInstance(spi);
		return vertex;
	}
	
	public static JobTaskVertex createTask(Class<? extends AbstractMinimalTask> task, JobGraph graph, int dop) {
		JobTaskVertex taskVertex = new JobTaskVertex(task.getName(), graph);
		taskVertex.setTaskClass(task);
		taskVertex.setNumberOfSubtasks(dop);
		
		return taskVertex;
	}
	
	public static void connectJobVertices(ShipStrategy shipStrategy, AbstractJobVertex outputVertex, 
			AbstractJobVertex inputVertex, int[] keyPos, Class<? extends Key>[] keyTypes) throws JobGraphDefinitionException {
		ChannelType channelType = null;

		switch (shipStrategy) {
		case FORWARD:
		case PARTITION_LOCAL_HASH:
			channelType = ChannelType.INMEMORY;
			break;
		case PARTITION_HASH:
		case PARTITION_RANGE:
		case BROADCAST:
		case SFR:
			channelType = ChannelType.NETWORK;
			break;
		default:
			throw new IllegalArgumentException("Unsupported ship-strategy: " + shipStrategy.name());
		}

		TaskConfig outputConfig = new TaskConfig(outputVertex.getConfiguration());
		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());

		// connect child with inmemory channel
		outputVertex.connectTo(inputVertex, channelType, CompressionLevel.NO_COMPRESSION);
		// set ship strategy in vertex and child

		// set strategies in task configs
		if ( (keyPos == null | keyTypes == null) || (keyPos.length == 0 | keyTypes.length == 0)) {
			outputConfig.addOutputShipStrategy(shipStrategy);
		} else {
			outputConfig.addOutputShipStrategy(shipStrategy, keyPos, keyTypes);
		}
		inputConfig.addInputShipStrategy(shipStrategy);
	}
	

	public static Configuration getConfiguration() {
		String location = null;
		if (System.getenv(ENV_CONFIG_DIRECTORY) != null) {
			location = System.getenv(ENV_CONFIG_DIRECTORY);
		} else {
			location = DEFAULT_CONFIG_DIRECTORY;
		}
		System.out.println(location);

		GlobalConfiguration.loadConfiguration(location);
		Configuration config = GlobalConfiguration.getConfiguration();

		return config;
	}
	
	public static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
		JobClient client = new JobClient(graph, nepheleConfig);
		client.submitJobAndWait();
	}
	
	public static class DummyNullOutput extends AbstractOutputTask {

		private NepheleReaderIterator input;

		@Override
		public void registerInputOutput() {
			this.input = new NepheleReaderIterator(new MutableRecordReader<PactRecord>(this, 
					new PointwiseDistributionPattern()));
		}

		@Override
		public void invoke() throws Exception {
			PactRecord rec = new PactRecord();
			while(input.next(rec)) {
				throw new RuntimeException("This task should not receive records");
			}
		}
		
	}
}
