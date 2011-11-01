package eu.stratosphere.pact.iterative.nephele;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.iterative.nephele.io.EdgeInput;
import eu.stratosphere.pact.iterative.nephele.io.EdgeOutput;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.iterative.nephele.tasks.CacheBuild;
import eu.stratosphere.pact.iterative.nephele.tasks.DuplicateEdges;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class NepheleDriver {
	// config dir parameters
	private static final String DEFAULT_CONFIG_DIRECTORY = "/home/mkaufmann/stratosphere-0.2/conf";
	private static final String ENV_CONFIG_DIRECTORY = "NEPHELE_CONF_DIR";
	
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		final int dop = Integer.parseInt(args[0]);
		final String input = args[1];
		final String output = args[2];
		
		JobGraph graph = new JobGraph("Iterative Test");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(EdgeInput.class, input, graph, dop);
		
		JobTaskVertex emitDirectedEdges = createTask(DuplicateEdges.class, graph, dop);
		emitDirectedEdges.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex buildCache = createTask(CacheBuild.class, graph, dop);
		buildCache.setVertexToShareInstancesWith(sourceVertex);
		setProperty(buildCache, CacheBuild.CACHE_ID_PARAM, "edges");
		
		JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, emitDirectedEdges, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, emitDirectedEdges, buildCache, 
				new int[] {0}, new Class[] {PactInteger.class});
		connectJobVertices(ShipStrategy.FORWARD, buildCache, sinkVertex, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
	
	private static void setProperty(JobTaskVertex buildCache,
			String key, String value) {
		TaskConfig taskConfig = new TaskConfig(buildCache.getConfiguration());
		taskConfig.setStubParameter(key, value);
		
	}

	private static JobInputVertex createInput(Class<? extends InputFormat<? extends InputSplit>> format, 
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
	
	private static JobOutputVertex createOutput(Class<? extends OutputFormat> format, String path,
			JobGraph graph, int dop) {
		JobOutputVertex sinkVertex = new JobOutputVertex("Output task", graph);
		sinkVertex.setOutputClass(DataSinkTask.class);
		sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, dop);
		
		TaskConfig sinkConfig = new TaskConfig(sinkVertex.getConfiguration());
		sinkConfig.setStubClass(format);
		sinkConfig.setLocalStrategy(LocalStrategy.NONE);
		sinkConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, path);
		
		return sinkVertex;
	}
	
	private static JobTaskVertex createTask(Class<? extends AbstractMinimalTask> task, JobGraph graph, int dop) {
		JobTaskVertex taskVertex = new JobTaskVertex(task.getName(), graph);
		taskVertex.setTaskClass(task);
		taskVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, dop);
		
		return taskVertex;
	}
	
	private static void connectJobVertices(ShipStrategy shipStrategy, AbstractJobVertex outputVertex, 
			AbstractJobVertex inputVertex, int[] keyPos, Class<? extends Key>[] keyTypes) throws JobGraphDefinitionException {
		ChannelType channelType = null;

		switch (shipStrategy) {
		case FORWARD:
		case PARTITION_LOCAL_HASH:
			channelType = ChannelType.INMEMORY;
			break;
		case PARTITION_HASH:
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
	

	private static Configuration getConfiguration() {
		String location = null;
		if (System.getenv(ENV_CONFIG_DIRECTORY) != null) {
			location = System.getenv(ENV_CONFIG_DIRECTORY);
		} else {
			location = DEFAULT_CONFIG_DIRECTORY;
		}

		GlobalConfiguration.loadConfiguration(location);
		Configuration config = GlobalConfiguration.getConfiguration();

		return config;
	}
	
	private static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
		JobClient client = new JobClient(graph, nepheleConfig);
		client.submitJobAndWait();
	}
}
