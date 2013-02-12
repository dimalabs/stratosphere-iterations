/**
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
 */

package eu.stratosphere.pact4s.tests.perf.plainScala

import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.`type`.PactRecord;
import eu.stratosphere.pact.common.`type`.base.PactDouble;
import eu.stratosphere.pact.common.`type`.base.PactInteger;
import eu.stratosphere.pact.common.`type`.base.PactLong;
import eu.stratosphere.pact.common.`type`.base.PactString;
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.`type`.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;

class TPCHQuery3 extends PlanAssembler with PlanAssemblerDescription {

  import TPCHQuery3._

  override def getPlan(args: String*): Plan = {
    null
  }

  override def getDescription() = "Parameters: [noSubStasks], [orders], [lineitem], [output]"
}

object TPCHQuery3 {
  
}