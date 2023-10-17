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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public class TestDDArchiveContainsFileType {

    @Test
    public void testArchive() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDArchiveContainsFileType());
        runner.setProperty(DDArchiveContainsFileType.FAIL_ON_SUB_ARCHIVES, "false");
        runner.setProperty(DDArchiveContainsFileType.ROUTE_TO_FAILURE, "false");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"application/pdf\",\"text/plain\"]");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("zip_inside_zip"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDArchiveContainsFileType.REL_SUCCESS, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDArchiveContainsFileType.REL_SUCCESS).get(0);
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "true");
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, "text/plain");
    }

    @Test
    public void testNonArchive() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDArchiveContainsFileType());
        runner.setProperty(DDArchiveContainsFileType.FAIL_ON_SUB_ARCHIVES, "false");
        runner.setProperty(DDArchiveContainsFileType.ROUTE_TO_FAILURE, "false");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"text/plain\"]");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("txt_file"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDArchiveContainsFileType.REL_SUCCESS, 1);
    }
}
