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
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public class TestDDExtractText {
    @Test
    public void test1() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, String.valueOf(false));
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(DDExtractText.REL_SUCCESS).get(0);
        mff.assertContentEquals("Hello World");
    }

    @Test
    public void test2() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, String.valueOf(false));
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("ms_excel_2007_xlsb"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
    }

    @Test
    public void testSizeLimit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, String.valueOf(false));
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "6 B");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(DDExtractText.REL_SUCCESS).get(0);
        mff.assertContentEquals("Hello");
        mff.assertAttributeExists(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED);
        mff.assertAttributeEquals(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, "true");
    }

    @Test
    public void testCharLimit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, String.valueOf(false));
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "4");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(DDExtractText.REL_SUCCESS).get(0);
        mff.assertContentEquals("Hell");
    }

    @Test
    public void encryptedOrPasswordProtectedFileTest() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("password_protected_pdf"));
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_FAILURE, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(DDExtractText.REL_FAILURE).get(0);
        mff.assertAttributeExists(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY);
        mff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, "io.github.deepakdaneva.nifi.exceptions.EncryptedOrPasswordProtectedFileException");
    }

    @Test
    public void zeroByteFile() {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, String.valueOf(false));
        runner.enqueue(new byte[0]);
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(DDExtractText.REL_SUCCESS).get(0);
        mff.assertAttributeNotExists(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED);
        mff.assertContentEquals("");
    }

    @Test
    public void oomXlsxTest() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new DDExtractText());
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("more_than_80mb_content_xlsx"));
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "50 MB");
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "-1");
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.setThreadCount(1);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(DDExtractText.REL_SUCCESS).get(0);
        mff.assertAttributeExists(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED);
        mff.assertAttributeEquals(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, "true");
    }
}
