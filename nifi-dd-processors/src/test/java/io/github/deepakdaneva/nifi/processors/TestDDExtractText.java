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
import io.github.deepakdaneva.nifi.exceptions.EncryptedOrPasswordProtectedFileException;
import io.github.deepakdaneva.nifi.exceptions.TextExtractionLimitReachedException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DDExtractText}.
 */
class TestDDExtractText {

    private static final String TIKA_CONFIG = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><properties><parsers><parser class=\"org.apache.tika.parser.DefaultParser\"/></parsers></properties>";

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new DDExtractText());
    }

    private MockFlowFile single(final org.apache.nifi.processor.Relationship relationship) {
        runner.assertAllFlowFilesTransferred(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    @Test
    void docx() throws IOException {
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_SUCCESS);
        ff.assertContentEquals("Hello World");
        ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "text/plain");
        ff.assertAttributeNotExists(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED);
        ff.assertAttributeNotExists(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY);
    }

    @Test
    void xlsb() throws IOException {
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("ms_excel_2007_xlsb"));
        runner.run();
        runner.assertAllFlowFilesTransferred(DDExtractText.REL_SUCCESS, 1);
    }

    @Test
    void pdf() throws IOException {
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("pdf_file"));
        runner.run();
        assertTrue(single(DDExtractText.REL_SUCCESS).getSize() > 0);
    }

    @Test
    void testSizeLimit() throws IOException {
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "6 B");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_SUCCESS);
        ff.assertContentEquals("Hello");
        ff.assertAttributeEquals(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, "true");
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, TextExtractionLimitReachedException.class.getName());
    }

    @Test
    void testCharLimit() throws IOException {
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "4");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("docx_file"));
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_SUCCESS);
        ff.assertContentEquals("Hell");
        ff.assertAttributeEquals(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, "true");
    }

    @Test
    void limitReachedRoutesOriginalToFailure() throws IOException {
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "4");
        final Path docx = DDTestConstants.TEST_FILES_PATH.resolve("docx_file");
        runner.enqueue(docx);
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_FAILURE);
        ff.assertContentEquals(docx);
        ff.assertAttributeEquals(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, "true");
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, TextExtractionLimitReachedException.class.getName());
    }

    @Test
    void sizeLimitCountsUtf8BytesAndOutputIsUtf8() {
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "5 B");
        // 'é' is two bytes in UTF-8, so only "aébc" fits in 5 bytes
        runner.enqueue("aébcd".getBytes(StandardCharsets.UTF_8));
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_SUCCESS);
        assertEquals("aébc", new String(ff.toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void whitespaceIsTrimmed() {
        runner.enqueue("\n\n  Hello   World \n\n".getBytes(StandardCharsets.UTF_8));
        runner.run();
        single(DDExtractText.REL_SUCCESS).assertContentEquals("Hello   World");
    }

    @Test
    void encryptedOrPasswordProtectedFileTest() throws IOException {
        final Path pdf = DDTestConstants.TEST_FILES_PATH.resolve("password_protected_pdf");
        runner.enqueue(pdf);
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_FAILURE);
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, EncryptedOrPasswordProtectedFileException.class.getName());
        ff.assertContentEquals(pdf);
    }

    @Test
    void zeroByteFile() {
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.enqueue(new byte[0]);
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_SUCCESS);
        ff.assertAttributeNotExists(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED);
        ff.assertContentEquals("");
    }

    @Test
    void oomXlsxTest() throws IOException {
        // the workbook expands to more than 80 MB of text; a small limit keeps the test fast while proving truncation
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("more_than_80mb_content_xlsx"));
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "5 MB");
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "-1");
        runner.setProperty(DDExtractText.FAIL_ON_LIMIT_REACHED, "false");
        runner.run();
        final MockFlowFile ff = single(DDExtractText.REL_SUCCESS);
        ff.assertAttributeEquals(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, "true");
        assertTrue(ff.getSize() <= 5L * 1024 * 1024);
    }

    @Test
    void tikaConfigBodyAndFile(@TempDir final Path dir) throws IOException {
        runner.setProperty(DDExtractText.TIKA_CONFIG_BODY, TIKA_CONFIG);
        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();
        single(DDExtractText.REL_SUCCESS).assertContentEquals("hello");

        final Path file = Files.writeString(dir.resolve("tika-config.xml"), TIKA_CONFIG);
        runner.setProperty(DDExtractText.TIKA_CONFIG_FILE, file.toString());
        runner.assertNotValid();
        runner.removeProperty(DDExtractText.TIKA_CONFIG_BODY);
        runner.assertValid();
    }

    @Test
    void invalidLimits() {
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "big");
        runner.assertNotValid();
        runner.setProperty(DDExtractText.EXTRACT_SIZE_LIMIT, "-1");
        runner.assertValid();
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "-5");
        runner.assertNotValid();
        runner.setProperty(DDExtractText.EXTRACT_CHARACTERS_LIMIT, "-1");
        runner.assertValid();
    }
}
