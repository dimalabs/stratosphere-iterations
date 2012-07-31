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

package eu.stratosphere.pact.example.relational;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;

/**
 * Implements the following relational OLAP query as PACT program:
 * 
 * <code><pre>
 * SELECT r.pageURL, r.pageRank, r.avgDuration
 * FROM Documents d JOIN Rankings r
 * 	ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords])
 * 	AND r.rank > [rank]
 * 	AND NOT EXISTS (
 * 		SELECT * FROM Visits v
 * 		WHERE v.destUrl = d.url
 * 			AND v.visitDate < [date]); 
 *  * </pre></code> 
 * 
 * Table Schemas: <code><pre>
 * CREATE TABLE Documents (
 * 					url VARCHAR(100) PRIMARY KEY,
 * 					contents TEXT );
 * 
 * CREATE TABLE Rankings (
 * 					pageRank INT,
 * 					pageURL VARCHAR(100) PRIMARY KEY,     
 * 					avgDuration INT );       
 * 
 * CREATE TABLE Visits (
 * 					sourceIP VARCHAR(16),
 * 					destURL VARCHAR(100),
 * 					visitDate DATE,
 * 					adRevenue FLOAT,
 * 					userAgent VARCHAR(64),
 * 					countryCode VARCHAR(3),
 * 					languageCode VARCHAR(6),
 * 					searchWord VARCHAR(32),
 * 					duration INT );
 * </pre></code>
 * 
 * @author Fabian Hueske
 */
public class WebLogAnalysis implements PlanAssembler, PlanAssemblerDescription
{
	// TODO JAVADOC !!!
	public static class WebLogAnalysisOutFormat extends FileOutputFormat {
		
		private final StringBuilder buffer = new StringBuilder();

		@Override
		public void writeRecord(PactRecord record) throws IOException {
			buffer.setLength(0);
			buffer.append(record.getField(1, PactInteger.class).toString());
			buffer.append('|');
			buffer.append(record.getField(0, PactString.class).toString());
			buffer.append('|');
			buffer.append(record.getField(2, PactInteger.class).toString());
			buffer.append('|');
			buffer.append('\n');
			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	/**
	 * MapStub that filters for documents that contain a certain set of
	 * keywords. 
	 * SameKey is annotated because the key is not changed 
	 * (key of the input set is equal to the key of the output set).
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	@ConstantFields(fields={0})
	@OutCardBounds(lowerBound=0, upperBound=1)
	public static class FilterDocs extends MapStub {
		
		private static final String[] KEYWORDS = { " editors ", " oscillations ", " convection " };
		
		/**
		 * Filters for documents that contain all of the given keywords and emit their keys.
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = record.getField(1, PactString.class).toString();
			boolean allContained = true;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					allContained = false;
					break;
				}
			}

			if (allContained) {
				record.setNumFields(1);
				out.collect(record);
			}
		}
	}

	/**
	 * MapStub that filters for records where the rank exceeds a certain threshold.
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	@ConstantFieldsExcept(fields={})
	public static class FilterRanks extends MapStub {
		
		private static final int RANKFILTER = 50;
		
		/**
		 * Filters for records of the rank relation where the rank is greater
		 * than the given threshold. The key is set to the URL of the record.
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			
			if (record.getField(1, PactInteger.class).getValue() > RANKFILTER) {
				out.collect(record);
			}			
		}
	}

	/**
	 * MapStub that filters for records of the visits relation where the year
	 * (from the date string) is equal to a certain value.
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	@ConstantFields(fields={0})
	public static class FilterVisits extends MapStub {

		private static final int YEARFILTER = 2010;
		

		/**
		 * Filters for records of the visits relation where the year of visit is equal to a
		 * specified value. The URL of all visit records passing the filter is emitted.
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			
			// Parse date string with the format YYYY-MM-DD and extract the year
			String dateString = record.getField(1, PactString.class).getValue();
			int year = Integer.parseInt(dateString.substring(0,4)); 
			
			if (year == YEARFILTER) {
				record.setNumFields(1);
				out.collect(record);
				
			}
		}
	}

	/**
	 * MatchStub that joins the filtered entries from the documents and the
	 * ranks relation. SameKey is annotated because the key of both inputs and
	 * the output is the same (URL).
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	@ConstantFieldsSecondExcept(fields={})
	public static class JoinDocRanks extends MatchStub {

		/**
		 * Collects all entries from the documents and ranks relation where the
		 * key (URL) is identical. The output consists of the old key (URL) and
		 * the attributes of the ranks relation.
		 */
		@Override
		public void match(PactRecord document, PactRecord rank, Collector<PactRecord> out) throws Exception {
			out.collect(rank);	
		}
	}

	/**
	 * CoGroupStub that realizes an anti-join.
	 * If the first input does not provide any pairs, all pairs of the second input are emitted.
	 * Otherwise, no pair is emitted.
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	@ConstantFieldsFirstExcept(fields={})
	public static class AntiJoinVisits extends CoGroupStub {

		/**
		 * If the visit iterator is empty, all pairs of the rank iterator are emitted.
		 * Otherwise, no pair is emitted. 
		 */
		@Override
		public void coGroup(Iterator<PactRecord> ranks, Iterator<PactRecord> visits, Collector<PactRecord> out) {
			// Check if there is a entry in the visits relation
			if (!visits.hasNext()) {
				while (ranks.hasNext()) {
					// Emit all rank pairs
					out.collect(ranks.next());
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks     = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String docsInput   = (args.length > 1 ? args[1] : "");
		String ranksInput  = (args.length > 2 ? args[2] : "");
		String visitsInput = (args.length > 3 ? args[3] : "");
		String output      = (args.length > 4 ? args[4] : "");

		// Create DataSourceContract for documents relation
		FileDataSource docs = new FileDataSource(RecordInputFormat.class, docsInput, "Docs Input");
		docs.setDegreeOfParallelism(noSubTasks);
		docs.getCompilerHints().setUniqueField(new FieldSet(0));
		
		docs.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		docs.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		docs.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);
		// url
		docs.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, VarLengthStringParser.class);
		docs.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 0);
		// doctext
		docs.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, VarLengthStringParser.class);
		docs.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 1);
		

		// Create DataSourceContract for ranks relation
		FileDataSource ranks = new FileDataSource(RecordInputFormat.class, ranksInput, "Ranks input");
		ranks.setDegreeOfParallelism(noSubTasks);
		
		ranks.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		ranks.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		ranks.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 3);
		// url
		ranks.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, VarLengthStringParser.class);
		ranks.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 1);
		// rank
		ranks.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		ranks.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 0);
		// avgDuration
		ranks.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+2, DecimalTextIntParser.class);
		ranks.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+2, 2);

		// Create DataSourceContract for visits relation
		FileDataSource visits = new FileDataSource(RecordInputFormat.class, visitsInput, "Visits input:q");
		visits.setDegreeOfParallelism(noSubTasks);
		
		visits.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		visits.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		visits.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);
		// url
		visits.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, VarLengthStringParser.class);
		visits.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 1);
		// date
		visits.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, VarLengthStringParser.class);
		visits.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 2);
		

		// Create MapContract for filtering the entries from the documents
		// relation
		MapContract filterDocs = new MapContract(FilterDocs.class, docs, "Filter Docs");
		filterDocs.setDegreeOfParallelism(noSubTasks);
		filterDocs.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.15f);
		filterDocs.getCompilerHints().setAvgBytesPerRecord(60);
		filterDocs.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0}), 1);

		// Create MapContract for filtering the entries from the ranks relation
		MapContract filterRanks = new MapContract(FilterRanks.class, ranks, "Filter Ranks");
		filterRanks.setDegreeOfParallelism(noSubTasks);
		filterRanks.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.25f);
		filterRanks.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0}), 1);

		// Create MapContract for filtering the entries from the visits relation
		MapContract filterVisits = new MapContract(FilterVisits.class, visits, "Filter Visits");
		filterVisits.setDegreeOfParallelism(noSubTasks);
		filterVisits.getCompilerHints().setAvgBytesPerRecord(60);
		filterVisits.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.2f);

		// Create MatchContract to join the filtered documents and ranks
		// relation
		MatchContract joinDocsRanks = new MatchContract(JoinDocRanks.class, PactString.class, 0, 0, filterDocs,
				filterRanks, "Join Docs Ranks");
		joinDocsRanks.setDegreeOfParallelism(noSubTasks);

		// Create CoGroupContract to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		CoGroupContract antiJoinVisits = new CoGroupContract(AntiJoinVisits.class, PactString.class, 0, 0, joinDocsRanks, 
				filterVisits, "Antijoin DocsVisits");
		antiJoinVisits.setDegreeOfParallelism(noSubTasks);
		antiJoinVisits.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.8f);

		// Create DataSinkContract for writing the result of the OLAP query
		FileDataSink result = new FileDataSink(WebLogAnalysisOutFormat.class, output, antiJoinVisits, "Result");
		result.setDegreeOfParallelism(noSubTasks);

		// Return the PACT plan
		return new Plan(result, "Weblog Analysis");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubTasks], [docs], [ranks], [visits], [output]";
	}

}
