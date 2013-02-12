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

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.example.datamining.KMeansIteration;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class KMeansIteration2ITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(KMeansIteration2ITCase.class);

	private final String DATAPOINTS = "0|50.90|16.20|72.08|\n" + "1|73.65|61.76|62.89|\n" + "2|61.73|49.95|92.74|\n"
			+ "3|1.60|70.11|16.32|\n" + "4|2.43|19.81|89.56|\n" + "5|67.99|9.00|14.48|\n" + "6|87.80|84.49|55.83|\n"
			+ "7|90.26|42.99|53.29|\n" + "8|51.36|6.16|9.35|\n" + "9|12.43|9.52|12.54|\n" + "10|80.01|8.78|29.74|\n"
			+ "11|92.76|2.93|80.07|\n" + "12|46.32|100.00|22.98|\n" + "13|34.11|45.61|58.60|\n"
			+ "14|68.82|16.36|96.60|\n" + "15|81.47|76.45|28.40|\n" + "16|65.55|40.21|43.43|\n"
			+ "17|84.22|88.56|13.31|\n" + "18|36.99|68.36|57.12|\n" + "19|28.87|37.69|91.04|\n"
			+ "20|31.56|13.22|86.00|\n" + "21|18.49|34.45|54.52|\n" + "22|13.33|94.02|92.07|\n"
			+ "23|91.19|81.62|55.06|\n" + "24|85.78|39.02|25.58|\n" + "25|94.41|47.07|78.23|\n"
			+ "26|90.62|10.43|80.20|\n" + "27|31.52|85.81|39.79|\n" + "28|24.65|77.98|26.35|\n"
			+ "29|69.34|75.79|63.96|\n" + "30|22.56|78.61|66.66|\n" + "31|91.74|83.82|73.92|\n"
			+ "32|76.64|89.53|44.66|\n" + "33|36.02|73.01|92.32|\n" + "34|87.86|18.94|10.74|\n"
			+ "35|91.94|34.61|5.20|\n" + "36|12.52|47.01|95.29|\n" + "37|44.01|26.19|78.50|\n"
			+ "38|26.20|73.36|10.08|\n" + "39|15.21|17.37|54.33|\n" + "40|27.96|94.81|44.41|\n"
			+ "41|26.44|44.81|70.88|\n" + "42|53.29|26.69|2.40|\n" + "43|23.94|11.50|1.71|\n"
			+ "44|19.00|25.48|50.80|\n" + "45|82.26|1.88|58.08|\n" + "46|47.56|82.54|82.73|\n"
			+ "47|51.54|35.10|32.95|\n" + "48|86.71|55.51|19.08|\n" + "49|54.16|23.68|32.41|\n"
			+ "50|71.81|32.83|46.66|\n" + "51|20.70|14.19|64.96|\n" + "52|57.17|88.56|55.23|\n"
			+ "53|91.39|49.38|70.55|\n" + "54|47.90|62.07|76.03|\n" + "55|55.70|37.77|30.15|\n"
			+ "56|87.87|74.62|25.95|\n" + "57|95.70|45.04|15.27|\n" + "58|41.61|89.37|24.45|\n"
			+ "59|82.19|20.84|11.13|\n" + "60|49.88|2.62|18.62|\n" + "61|16.42|53.30|74.13|\n"
			+ "62|38.37|72.62|35.16|\n" + "63|43.26|49.59|92.56|\n" + "64|28.96|2.36|78.49|\n"
			+ "65|88.41|91.43|92.55|\n" + "66|98.61|79.58|33.03|\n" + "67|4.94|18.65|30.78|\n"
			+ "68|75.89|79.30|63.90|\n" + "69|93.18|76.26|9.50|\n" + "70|73.43|70.50|76.49|\n"
			+ "71|78.64|90.87|34.49|\n" + "72|58.47|63.07|8.82|\n" + "73|69.74|54.36|64.43|\n"
			+ "74|38.47|36.60|33.39|\n" + "75|51.07|14.75|2.54|\n" + "76|24.18|16.85|15.00|\n"
			+ "77|7.56|50.72|93.45|\n" + "78|64.28|97.01|57.31|\n" + "79|85.30|24.13|76.57|\n"
			+ "80|72.78|30.78|13.11|\n" + "81|18.42|17.45|32.20|\n" + "82|87.44|74.98|87.90|\n"
			+ "83|38.30|17.77|37.33|\n" + "84|63.62|7.90|34.23|\n" + "85|8.84|67.87|30.65|\n"
			+ "86|76.12|51.83|80.12|\n" + "87|32.30|74.79|4.39|\n" + "88|41.73|45.34|18.66|\n"
			+ "89|58.13|18.43|83.38|\n" + "90|98.10|33.46|83.07|\n" + "91|17.76|4.10|88.51|\n"
			+ "92|60.58|18.15|59.96|\n" + "93|50.11|33.25|85.64|\n" + "94|97.74|60.93|38.97|\n"
			+ "95|76.31|52.50|95.43|\n" + "96|7.71|85.85|36.26|\n" + "97|9.32|72.21|42.17|\n"
			+ "98|71.29|51.88|57.62|\n" + "99|31.39|7.27|88.74|";

	private final String CLUSTERCENTERS = "0|1.96|65.04|20.82|\n" + "1|53.99|84.23|81.59|\n" + "2|97.28|74.50|40.32|\n"
			+ "3|63.57|24.53|87.07|\n" + "4|28.10|43.27|86.53|\n" + "5|99.51|62.70|64.48|\n" + "6|30.31|30.36|80.46|";

	private final String NEWCLUSTERCENTERS = "0|28.47|54.80|21.88|\n" + "1|52.74|80.10|73.03|\n"
			+ "2|83.92|60.45|25.17|\n" + "3|70.73|20.18|67.06|\n" + "4|22.51|47.19|86.23|\n" + "5|82.70|53.79|68.68|\n"
			+ "6|29.74|19.17|59.16|";

	protected String dataPath = null;
	protected String clusterPath = null;
	protected String resultPath = null;

	public KMeansIteration2ITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {

		dataPath = getFilesystemProvider().getTempDirPath() + "/dataPoints";
		clusterPath = getFilesystemProvider().getTempDirPath() + "/iter_0";
		resultPath = getFilesystemProvider().getTempDirPath() + "/iter_1";
		
		int noPartitions = 4;

		// create data path
		getFilesystemProvider().createDir(dataPath);
		
		String[] splits = splitInputString(DATAPOINTS, '\n', noPartitions);

		int i = 0;
		for(String split : splits) {
			getFilesystemProvider().createFile(dataPath + "/part_" + i++ + ".txt", split);
			LOG.debug("DATAPOINT split "+i+": \n>" + split + "<");
		}
		
		// create cluster path and copy data
		getFilesystemProvider().createDir(clusterPath);
		getFilesystemProvider().createFile(clusterPath+"/1", CLUSTERCENTERS);
		LOG.debug("Clusters: \n>" + CLUSTERCENTERS + "<");

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		KMeansIteration kmi = new KMeansIteration();

		Plan plan = kmi.getPlan(config.getString("KMeansIterationTest#NoSubtasks", "1"), 
				getFilesystemProvider().getURIPrefix()	+ dataPath, 
				getFilesystemProvider().getURIPrefix() + clusterPath,  
				getFilesystemProvider().getURIPrefix()	+ resultPath);
		
		setParameterToCross(plan, "INPUT_LEFT_SHIP_STRATEGY", "SHIP_FORWARD");

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception {

		Comparator<String> deltaComp = new Comparator<String>() {

			private final double DELTA = 0.1;
			
			@Override
			public int compare(String o1, String o2) {
				
				StringTokenizer st1 = new StringTokenizer(o1, "|");
				StringTokenizer st2 = new StringTokenizer(o2, "|");
				
				if(st1.countTokens() != st2.countTokens()) {
					return st1.countTokens() - st2.countTokens();
				}
				
				// first token is ID
				String t1 = st1.nextToken();
				String t2 = st2.nextToken();
				if(!t1.equals(t2)) {
					return t1.compareTo(t2);
				}
				
				while(st1.hasMoreTokens()) {
					t1 = st1.nextToken();
					t2 = st2.nextToken();
					
					double d1 = Double.parseDouble(t1);
					double d2 = Double.parseDouble(t2);
					
					if(Math.abs(d1-d2) > DELTA) {
						return d1 < d2 ? -1 : 1;
					}
				}
				
				return 0;
			}
		};
		
		// Test results
		compareResultsByLinesInMemory(NEWCLUSTERCENTERS, resultPath, deltaComp);

		// clean up file
		getFilesystemProvider().delete(dataPath, true);
		getFilesystemProvider().delete(clusterPath, true);
		getFilesystemProvider().delete(resultPath, true);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("KMeansIterationTest#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}

	private String[] splitInputString(String inputString, char splitChar, int noSplits) {

		String splitString = inputString.toString();
		String[] splits = new String[noSplits];
		int partitionSize = (splitString.length() / noSplits) - 2;

		// split data file and copy parts
		for (int i = 0; i < noSplits - 1; i++) {
			int cutPos = splitString.indexOf(splitChar, (partitionSize < splitString.length() ? partitionSize
				: (splitString.length() - 1)));
			splits[i] = splitString.substring(0, cutPos) + "\n";
			splitString = splitString.substring(cutPos + 1);
		}
		splits[noSplits - 1] = splitString;

		return splits;

	}

	public static class KMeanDataGenerator {

		int noPoints;
		int noClusters;
		int noDims;

		Random rand = new Random(System.currentTimeMillis());

		double[][] dataPoints;
		double[][] centers;
		double[][] newCenters;

		public KMeanDataGenerator(int noPoints, int noClusters, int noDims) {
			this.noPoints = noPoints;
			this.noClusters = noClusters;
			this.noDims = noDims;

			this.dataPoints = new double[noPoints][noDims];
			this.centers = new double[noClusters][noDims];
			this.newCenters = new double[noClusters][noDims];

			// init data points
			for (int i = 0; i < noPoints; i++) {
				for (int j = 0; j < noDims; j++) {
					dataPoints[i][j] = rand.nextDouble() * 100;
				}
			}

			// init centers
			for (int i = 0; i < noClusters; i++) {
				for (int j = 0; j < noDims; j++) {
					centers[i][j] = rand.nextDouble() * 100;
				}
			}

			// compute new centers
			int[] dataPointCnt = new int[noClusters];

			for (int i = 0; i < noPoints; i++) {

				double minDist = Double.MAX_VALUE;
				int nearestCluster = 0;
				for (int j = 0; j < noClusters; j++) {
					double dist = computeDistance(dataPoints[i], centers[j]);
					if (dist < minDist) {
						minDist = dist;
						nearestCluster = j;
					}
				}

				for (int k = 0; k < noDims; k++) {
					newCenters[nearestCluster][k] += dataPoints[i][k];
				}
				dataPointCnt[nearestCluster]++;

			}

			for (int i = 0; i < noClusters; i++) {
				for (int j = 0; j < noDims; j++) {
					newCenters[i][j] /= (dataPointCnt[i] != 0 ? dataPointCnt[i] : 1);
				}
			}
		}

		public String getDataPoints() {
			return points2String(this.dataPoints);
		}

		public String getClusterCenters() {
			return points2String(centers);
		}

		public String getNewClusterCenters() {
			return points2String(newCenters);
		}

		private String points2String(double[][] points) {
			StringBuilder sb = new StringBuilder();
			DecimalFormat df = new DecimalFormat("#.00");

			for (int i = 0; i < points.length; i++) {
				sb.append(i);
				sb.append('|');
				for (int j = 0; j < points[i].length; j++) {
					sb.append(df.format(points[i][j]));
					sb.append('|');
				}
				sb.append('\n');
			}
			return sb.toString();
		}

		private double computeDistance(double[] a, double[] b) {
			double sqrdSum = 0.0;
			for (int i = 0; i < a.length; i++) {
				sqrdSum += Math.pow(a[i] - b[i], 2);
			}
			return Math.sqrt(sqrdSum);
		}

	}
	
	public static void setParameterToCross(Plan p, String key, String value) {
		GenericDataSink sink = p.getDataSinks().iterator().next();
		ReduceContract reduce2 = (ReduceContract) sink.getInputs().get(0);
		ReduceContract reduce1 = (ReduceContract) reduce2.getInputs().get(0);
		CrossContract cross = (CrossContract) reduce1.getInputs().get(0);
		cross.getParameters().setString(key, value);
	}
}
