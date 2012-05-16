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

import eu.stratosphere.pact.common.generic.GenericMapper;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * Map task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapStub
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that 
 * to the <code>map()</code> method of the MapStub.
 *
 * @see MapStub
 * @see GenericMapper
 *
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class MapTask<IT, OT> extends AbstractPactTask<GenericMapper<IT, OT>, OT>
{
    /* (non-Javadoc)
      * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
      */
    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    /* (non-Javadoc)
      * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
      */
    @Override
    public Class<GenericMapper<IT, OT>> getStubType()
    {
        @SuppressWarnings("unchecked")
        final Class<GenericMapper<IT, OT>> clazz = (Class<GenericMapper<IT, OT>>) (Class<?>) GenericMapper.class;
        return clazz;
    }

    /* (non-Javadoc)
      * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
      */
    @Override
    public boolean requiresComparatorOnInput() {
        return false;
    }

    /* (non-Javadoc)
      * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
      */
    @Override
    public void prepare() throws Exception {
        // nothing, since a mapper does not need any preparation

    }

    /* (non-Javadoc)
      * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
      */
    @Override
    public void run() throws Exception
    {
        // cache references on the stack
        final MutableObjectIterator<IT> input = getInput(0);
        final GenericMapper<IT, OT> stub = this.stub;
        final Collector<OT> output = this.output;

        final IT record = this.<IT>getInputSerializer(0).createInstance();

        while (this.running && input.next(record)) {
            stub.map(record, output);
        }
    }

    /* (non-Javadoc)
      * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
      */
    @Override
    public void cleanup() throws Exception {
        // mappers need no cleanup, since no strategies are used.
    }
}