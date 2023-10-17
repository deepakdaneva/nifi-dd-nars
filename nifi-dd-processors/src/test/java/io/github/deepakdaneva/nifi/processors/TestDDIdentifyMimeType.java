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
package io.github.deepakdaneva.nifi.processors;

import io.github.deepakdaneva.nifi.DDTestConstants;
import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public class TestDDIdentifyMimeType {
    @Test
    public void test() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDIdentifyMimeType());
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("txt_file"));
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("pdf_file"));
        runner.setThreadCount(1);
        runner.run(2);
        runner.assertAllFlowFilesTransferred(DDIdentifyMimeType.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).forEach(ff -> {
            if (ff.getAttribute("filename").equals("txt_file")) {
                ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "text/plain");
            } else if (ff.getAttribute("filename").equals("pdf_file")) ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/pdf");
        });
    }
}
