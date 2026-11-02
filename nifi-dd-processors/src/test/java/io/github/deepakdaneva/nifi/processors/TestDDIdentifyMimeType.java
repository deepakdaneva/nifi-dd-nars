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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.github.deepakdaneva.nifi.TestArchives.tar;
import static io.github.deepakdaneva.nifi.TestArchives.entries;
import static io.github.deepakdaneva.nifi.TestArchives.utf8;

/**
 * Tests for {@link DDIdentifyMimeType}.
 */
class TestDDIdentifyMimeType {

    private static final String CUSTOM_MIMES = """
            <?xml version="1.0" encoding="UTF-8"?>
            <mime-info>
              <mime-type type="application/x-dd-test">
                <magic priority="50"><match value="DDTEST" type="string" offset="0"/></magic>
                <glob pattern="*.ddt"/>
              </mime-type>
            </mime-info>
            """;

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new DDIdentifyMimeType());
    }

    @Test
    void test() throws IOException {
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("txt_file"));
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("pdf_file"));
        runner.run(2);
        runner.assertAllFlowFilesTransferred(DDIdentifyMimeType.REL_SUCCESS, 2);
        final MockFlowFile txt = runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0);
        final MockFlowFile pdf = runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(1);
        txt.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "text/plain");
        txt.assertAttributeEquals(DDIdentifyMimeType.DD_MIME_EXTENSION, ".txt");
        pdf.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/pdf");
        pdf.assertAttributeEquals(DDIdentifyMimeType.DD_MIME_EXTENSION, ".pdf");
    }

    @Test
    void containerFormat() throws IOException {
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.run();
        runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0).assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
    }

    @Test
    void gzipExtension() {
        runner.enqueue(tar(entries("a.txt", utf8("a")), 0644, true));
        runner.run();
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0);
        ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/gzip");
        ff.assertAttributeEquals(DDIdentifyMimeType.DD_MIME_EXTENSION, ".gz");
    }

    @Test
    void emptyContent() {
        runner.enqueue(new byte[0]);
        runner.run();
        runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0).assertAttributeExists(DDCoreAttributes.MIME_TYPE);
    }

    @Test
    void customConfigBody() {
        runner.setProperty(DDIdentifyMimeType.MIME_CONFIG_BODY, CUSTOM_MIMES);
        runner.enqueue("DDTEST payload".getBytes(StandardCharsets.UTF_8));
        runner.run();
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0);
        ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/x-dd-test");
        ff.assertAttributeEquals(DDIdentifyMimeType.DD_MIME_EXTENSION, ".ddt");
    }

    @Test
    void customConfigFile(@TempDir final Path dir) throws IOException {
        runner.setProperty(DDIdentifyMimeType.MIME_CONFIG_FILE, Files.writeString(dir.resolve("mimes.xml"), CUSTOM_MIMES).toString());
        runner.enqueue("DDTEST payload".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0).assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/x-dd-test");

        runner.setProperty(DDIdentifyMimeType.MIME_CONFIG_BODY, CUSTOM_MIMES);
        runner.assertNotValid();
    }

    @Test
    void parametersKeptWhenNotCleaning() {
        runner.setProperty(DDIdentifyMimeType.CLEAN_MIME_TYPE_ONLY, "false");
        runner.setProperty(DDIdentifyMimeType.MIME_CONFIG_BODY, """
                <?xml version="1.0" encoding="UTF-8"?>
                <mime-info>
                  <mime-type type="application/x-dd-test; version=2">
                    <magic priority="50"><match value="DDTEST" type="string" offset="0"/></magic>
                  </mime-type>
                </mime-info>
                """);
        runner.enqueue("DDTEST".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.getFlowFilesForRelationship(DDIdentifyMimeType.REL_SUCCESS).get(0).assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/x-dd-test; version=2");
    }
}
