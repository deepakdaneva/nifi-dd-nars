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
package io.github.deepakdaneva.nifi.services;

import io.github.deepakdaneva.nifi.core.DDConstants;
import io.github.deepakdaneva.nifi.services.drop.FlowFileDropEventConsumer;
import io.github.deepakdaneva.nifi.services.drop.FlowFileDropEventService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
@Tags({DDConstants.DEEPAKDANEVA, "flowfile", "drop", "events", "node"})
@CapabilityDescription("Service to provide FlowFile drop events to the registered components, for the current node of the cluster.")
public class DDNodeFlowFileDropEventService extends AbstractControllerService implements FlowFileDropEventService {

    public static final PropertyDescriptor THREADS_COUNT = new PropertyDescriptor.Builder().name("Threads").description("Specify the number of threads to use when pushing the events to consumers, if more threads are provided then events will be provided to multiple consumers at a time increasing the throughput but should be used with caution as it will slow down the application if more threads are configured. This value can not be less than 2 and greator than 10.").required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).addValidator(StandardValidators.createLongValidator(2, 10, true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("2").build();

    private static ExecutorService executorService;
    private static Set<FlowFileDropEventConsumer> registeredConsumers;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(THREADS_COUNT);
    }

    @Override
    protected void init(ControllerServiceInitializationContext config) {
        registeredConsumers = new ConcurrentSkipListSet<>();
    }

    /**
     * @param context the configuration context.
     * @throws InitializationException if unable to initialize.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        executorService = Executors.newWorkStealingPool(context.getProperty(THREADS_COUNT).evaluateAttributeExpressions().asInteger());
    }

    @OnDisabled
    public void onDisabled() {
        executorService.shutdown();
        registeredConsumers.clear();
    }

    /**
     * Any consumer who wants to recieve dropped {@link FlowFile} must register themself using this register method first.
     *
     * @param consumer who in interested in recieving the FlowFile which is dropped by one of the supported component.
     */
    @Override
    public void register(FlowFileDropEventConsumer consumer) {
        if (Objects.isNull(consumer)) {
            throw new IllegalArgumentException("consumer can not be null");
        }
        registeredConsumers.add(consumer);
    }

    /**
     * FlowFile which is dropped by any of the supported component should be supplied through this method so that consumers will receive this supplied FlowFile.
     *
     * @param droppedFlowFile which should be provided to all consumers.
     */
    @Override
    public void supply(FlowFile droppedFlowFile) {
        executorService.submit(() -> {
            registeredConsumers.parallelStream().forEach(fc -> {
                try {
                    fc.accept(droppedFlowFile);
                } catch (Exception e) {
                    getLogger().error("Error faced by FlowFile consumer {} accepting the FlowFile {}", e, fc, droppedFlowFile);
                }
            });
        });
    }

    /**
     * FlowFiles which are dropped by any of the supported component should be supplied through this method so that consumers will receive these supplied FlowFiles. This method should be used in place of {@link #supply(FlowFile)} to get the maximum throughput for the dropped flowfile events and providing the dropped flowfiles altogether instead of individual events.
     *
     * @param droppedFlowFiles which should be provided to all consumers.
     */
    @Override
    public void supply(List<FlowFile> droppedFlowFiles) {
        executorService.submit(() -> {
            registeredConsumers.parallelStream().forEach(fc -> {
                try {
                    fc.accept(droppedFlowFiles);
                } catch (Exception e) {
                    getLogger().error("Error faced by FlowFiles consumer {} accepting the list of FlowFiles {}", e, fc, droppedFlowFiles);
                }
            });
        });
    }
}