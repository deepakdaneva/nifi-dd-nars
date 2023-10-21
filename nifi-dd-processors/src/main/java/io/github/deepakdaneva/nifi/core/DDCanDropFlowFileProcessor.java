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
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public abstract class DDCanDropFlowFileProcessor extends DDBaseProcessor {

    public static final String DROP_SIGNAL_CACHE_SERVICE_PROP_NAME = "Drop Signal Cache Service";
    public static final String FLOWFILE_DROPPED_RELATIONSHIP_NAME = "flowfile.dropped";

    public static final Relationship FLOWFILE_DROPPED = new Relationship.Builder().name(FLOWFILE_DROPPED_RELATIONSHIP_NAME).description("A FlowFile with a matching drop signal in the cache will be routed to this relationship").build();

    public static final PropertyDescriptor DROP_SIGNAL_DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder().name(DROP_SIGNAL_CACHE_SERVICE_PROP_NAME).description("The Controller Service that is used to check for FlowFile drop signals").required(false).identifiesControllerService(AtomicDistributedMapCacheClient.class).build();
    public static final PropertyDescriptor DROP_SIGNAL_IDENTIFIER = new PropertyDescriptor.Builder().name("Drop Signal Identifier").required(true).dependsOn(DROP_SIGNAL_DISTRIBUTED_CACHE_SERVICE).description("A value that specifies the key to a specific drop signal cache. To decide whether the FlowFile that is being processed by the processor should be sent to the '" + FLOWFILE_DROPPED.getName() + "' relationship, the processor checks the signals in the cache specified by this key").addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true)).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    public static final PropertyDescriptor GROUP_DROP_SIGNAL_IDENTIFIER = new PropertyDescriptor.Builder().name("Group Drop Signal Identifier").required(true).dependsOn(DROP_SIGNAL_DISTRIBUTED_CACHE_SERVICE).description("A value that specifies the key to a specific group drop signal cache. To decide whether the FlowFile of that specific group that is being processed by the processor should be sent to the '" + FLOWFILE_DROPPED.getName() + "' relationship, the processor checks the signals in the cache specified by this key").addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true)).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>(new HashSet<>());
    private boolean DROP_CONFIGURED;

    /**
     * Provides the ability to perform initialization logic
     *
     * @param context in which to perform initializaiton
     */
    @Override
    protected final void initComponent(ProcessorInitializationContext context) {
        initProcessor(context);
    }

    /**
     * Provides the ability to perform initialization logic for child component
     *
     * @param context in which to perform initializaiton
     */
    protected abstract void initProcessor(ProcessorInitializationContext context);

    /**
     * Hook method allowing subclasses to eagerly react to a configuration
     * change for the given property descriptor. As an alternative to using this
     * method a processor may simply get the latest value whenever it needs it
     * and if necessary lazily evaluate it.
     *
     * @param descriptor of the modified property
     * @param oldValue non-null property value (previous)
     * @param newValue the new property value or if null indicates the property
     * was removed
     */
    @Override
    public final void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        onPropertyChanged(descriptor, oldValue, newValue);
        if (DROP_SIGNAL_DISTRIBUTED_CACHE_SERVICE.equals(descriptor)) {
            DROP_CONFIGURED = !Objects.isNull(newValue);
        }
    }

    /**
     * Method invoked when any of the property of the component is changed
     * 
     * @param descriptor property descriptor
     * @param oldValue old value of the property
     * @param newValue new value of the property
     */
    public abstract void onPropertyChanged(PropertyDescriptor descriptor, String oldValue, String newValue);

    /**
     * All properties that this component supports.
     * 
     * @return a {@link List} of all {@link PropertyDescriptor}s that this component supports.
     */
    @Override
    protected final List<PropertyDescriptor> getAllProperties() {
        List<PropertyDescriptor> p = new ArrayList<>();
        if (Objects.nonNull(getExtraProperties()) && !getExtraProperties().isEmpty()) {
            p.addAll(getExtraProperties());
        }
        p.add(DROP_SIGNAL_DISTRIBUTED_CACHE_SERVICE);
        p.add(DROP_SIGNAL_IDENTIFIER);
        p.add(GROUP_DROP_SIGNAL_IDENTIFIER);
        return Collections.unmodifiableList(p);
    }

    /**
     * All properties that the child component supports
     * 
     * @return a {@link List} of all {@link PropertyDescriptor}s that needs to be appended to the existing parent property descriptors.
     */
    protected abstract List<PropertyDescriptor> getExtraProperties();

    /**
     * All relationships that this component supports
     * 
     * @return a {@link Set} of all {@link Relationship}s this component supports.
     */
    @Override
    public final Set<Relationship> getAllRelations() {
        Set<Relationship> s = new HashSet<>();
        if (Objects.nonNull(getExtraRelations()) && !getExtraRelations().isEmpty()) {
            s.addAll(getExtraRelations());
        }
        if (DROP_CONFIGURED) {
            s.add(FLOWFILE_DROPPED);
        }
        relationships.set(s);
        return relationships.get();
    }

    /**
     * All relationship that the child component supports
     * 
     * @return a {@link Set} of {@link Relationship}s that needs to be appended to the existing parent relationships.
     */
    public abstract Set<Relationship> getExtraRelations();

    /**
     * If no drop signal found for the FlowFile then this method will delegate the execution to {@link #execute(ProcessContext, ProcessSession)} method which must be implemented by the child component.
     *
     * @param processContext process context.
     * @param processSession process session.
     * @throws ProcessException if any exception is raised.
     */
    @Override
    public final void process(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        if (processContext.getProperty(DROP_SIGNAL_DISTRIBUTED_CACHE_SERVICE).isSet()) {
            //TODO: Implement business logic here to check the signal and drop/transfer the flowfile
            execute(processContext, processSession);
        } else {
            execute(processContext, processSession);
        }
    }

    /**
     * Actual processing by the processor in case FlowFile is not dropped.
     *
     * @param context process context.
     * @param session process session.
     * @throws ProcessException if any exception is raised.
     */
    public abstract void execute(ProcessContext context, ProcessSession session) throws ProcessException;

}
