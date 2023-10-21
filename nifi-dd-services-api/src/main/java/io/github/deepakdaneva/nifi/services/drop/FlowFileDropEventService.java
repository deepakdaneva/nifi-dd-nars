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
package io.github.deepakdaneva.nifi.services.drop;

import io.github.deepakdaneva.nifi.core.DDConstants;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;

import java.util.List;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
@Tags({DDConstants.DEEPAKDANEVA, "flowfile", "drop", "events"})
@CapabilityDescription("Service to provide FlowFile drop events to the registered components.")
public interface FlowFileDropEventService extends ControllerService {

    /**
     * Any consumer who wants to recieve dropped {@link org.apache.nifi.flowfile.FlowFile} must register themself using this register method first.
     *
     * @param consumer who in interested in recieving the FlowFile which is dropped by one of the supported component.
     */
    void register(FlowFileDropEventConsumer consumer);

    /**
     * FlowFile which is dropped by any of the supported component should be supplied through this method so that consumers will receive this supplied FlowFile.
     *
     * @param droppedFlowFile which should be provided to all consumers.
     */
    void supply(FlowFile droppedFlowFile);

    /**
     * FlowFiles which are dropped by any of the supported component should be supplied through this method so that consumers will receive these supplied FlowFiles. This method should be used in place of {@link #supply(FlowFile)} to get the maximum throughput for the dropped flowfile events and providing the dropped flowfiles altogether instead of individual events.
     *
     * @param droppedFlowFiles which should be provided to all consumers.
     */
    void supply(List<FlowFile> droppedFlowFiles);

}
