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

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.flowfile.FlowFile;

import java.util.List;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public interface FlowFileDropEventConsumer extends ConfigurableComponent {

    /**
     * Accept the individual dropped FlowFile and perform action as needed.
     *
     * @param droppedFlowFile by one of the supported component.
     */
    void accept(FlowFile droppedFlowFile);

    /**
     * Accept the list of dropped FlowFiles and perform action as needed.
     *
     * @param droppedFlowFiles by one of the supported component.
     */
    void accept(List<FlowFile> droppedFlowFiles);
}
