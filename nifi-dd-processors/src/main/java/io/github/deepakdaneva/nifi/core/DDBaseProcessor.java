/*
 * Copyright (C) 2023 Deepak Kumar Jangir
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.github.deepakdaneva.nifi.core;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Set;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public abstract class DDBaseProcessor extends AbstractProcessor {

    /**
     * Provides the ability to perform initialization logic
     *
     * @param context in which to perform initialization
     */
    @Override
    protected final void init(ProcessorInitializationContext context) {
        initComponent(context);
    }

    /**
     * Provides the ability to perform initialization logic for child component
     *
     * @param context in which to perform initializaiton
     */
    protected abstract void initComponent(ProcessorInitializationContext context);

    /**
     * All relationships of the final component
     * 
     * @return All relationships that the final component supports
     */
    @Override
    public final Set<Relationship> getRelationships() {
        return getAllRelations();
    }

    /**
     * All propertys of the final component
     * 
     * @return List of all the properties that the final component supports
     */
    @Override
    protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return getAllProperties();
    }

    /**
     * Delegates actual execution to {@link #process(ProcessContext, ProcessSession)}
     *
     * @param processContext process context.
     * @param processSession process session.
     * @throws ProcessException if any exception is raised.
     */
    @Override
    public final void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        process(processContext, processSession);
    }

    /**
     * Actual processing done by the child processor.
     *
     * @param processContext process context.
     * @param processSession process session.
     * @throws ProcessException if any exception is raised.
     */
    public abstract void process(ProcessContext processContext, ProcessSession processSession) throws ProcessException;

    /**
     * All properties that child component supports
     * 
     * @return a {@link List} of all {@link PropertyDescriptor}s that the child component supports.
     */
    protected abstract List<PropertyDescriptor> getAllProperties();

    /**
     * All relationships that child component supports
     * 
     * @return {@link Set} of all {@link Relationship}s that the child component supports.
     */
    public abstract Set<Relationship> getAllRelations();
}
