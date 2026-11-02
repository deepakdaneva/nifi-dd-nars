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

import io.github.deepakdaneva.commons.archive.NotAnArchiveOrSupportedArchiveException;
import io.github.deepakdaneva.nifi.DDTestConstants;
import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.github.deepakdaneva.nifi.TestArchives.entries;
import static io.github.deepakdaneva.nifi.TestArchives.sevenZ;
import static io.github.deepakdaneva.nifi.TestArchives.tar;
import static io.github.deepakdaneva.nifi.TestArchives.utf8;
import static io.github.deepakdaneva.nifi.TestArchives.zip;

/**
 * Tests for {@link DDArchiveContainsFileType}.
 */
class TestDDArchiveContainsFileType {

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new DDArchiveContainsFileType());
    }

    private MockFlowFile single(final org.apache.nifi.processor.Relationship relationship) {
        runner.assertAllFlowFilesTransferred(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    @Test
    void testArchive() throws IOException {
        runner.setProperty(DDArchiveContainsFileType.FAIL_ON_SUB_ARCHIVES, "false");
        runner.setProperty(DDArchiveContainsFileType.ROUTE_TO_FAILURE, "false");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"application/pdf\",\"text/plain\"]");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("zip_inside_zip"));
        runner.run();
        final MockFlowFile ff = single(DDArchiveContainsFileType.REL_SUCCESS);
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_MIME_TYPE, "application/zip");
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "true");
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, "text/plain");
    }

    @Test
    void testNonArchive() throws IOException {
        runner.setProperty(DDArchiveContainsFileType.ROUTE_TO_FAILURE, "false");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"text/plain\"]");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("txt_file"));
        runner.run();
        final MockFlowFile ff = single(DDArchiveContainsFileType.REL_SUCCESS);
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "false");
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, NotAnArchiveOrSupportedArchiveException.class.getName());
    }

    @Test
    void nonArchiveToFailureByDefault() throws IOException {
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"text/plain\"]");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("pdf_file"));
        runner.run();
        final MockFlowFile ff = single(DDArchiveContainsFileType.REL_FAILURE);
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_MIME_TYPE, "application/pdf");
        ff.assertAttributeNotExists(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND);
    }

    @Test
    void noMatch() {
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"application/pdf\"]");
        runner.enqueue(tar(entries("a.txt", utf8("hello")), 0644, true));
        runner.run();
        final MockFlowFile ff = single(DDArchiveContainsFileType.REL_SUCCESS);
        ff.assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "false");
        ff.assertAttributeNotExists(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE);
    }

    @Test
    void nestedMatchRequiresRecursion() {
        final byte[] archive = zip("inner.zip", zip("doc.xml", utf8("<?xml version=\"1.0\"?><a/>")));
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"application/xml\"]");
        runner.enqueue(archive);
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS).assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "false");

        runner.clearTransferState();
        runner.setProperty(DDArchiveContainsFileType.RECURSE_SUB_ARCHIVES, "true");
        runner.enqueue(archive);
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS).assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, "application/xml");
    }

    @Test
    void nestedArchiveTypeItselfMatches() {
        runner.setProperty(DDArchiveContainsFileType.RECURSE_SUB_ARCHIVES, "true");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"application/x-7z-compressed\"]");
        runner.enqueue(zip("inner.7z", sevenZ(entries("a.txt", utf8("a")))));
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS).assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, "application/x-7z-compressed");
    }

    @Test
    void nestedSevenZip() {
        runner.setProperty(DDArchiveContainsFileType.RECURSE_SUB_ARCHIVES, "true");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"text/plain\"]");
        runner.enqueue(zip("inner.7z", sevenZ(entries("a.txt", utf8("hello")))));
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS).assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, "text/plain");
    }

    @Test
    void maximumDepthStopsRecursion() {
        final byte[] archive = zip("l2.zip", zip("l3.zip", zip("a.txt", utf8("hello"))));
        runner.setProperty(DDArchiveContainsFileType.RECURSE_SUB_ARCHIVES, "true");
        runner.setProperty(DDArchiveContainsFileType.MAX_SUB_ARCHIVE_DEPTH, "2");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"text/plain\"]");
        runner.enqueue(archive);
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS).assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "false");
    }

    @Test
    void corruptSubArchive() {
        final byte[] archive = zip(entries("ok.txt", utf8("ok"), "bad.zip", utf8("PK\u0003\u0004 this is not really a zip")));
        runner.setProperty(DDArchiveContainsFileType.RECURSE_SUB_ARCHIVES, "true");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"application/pdf\"]");
        runner.enqueue(archive);
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS);

        runner.clearTransferState();
        runner.setProperty(DDArchiveContainsFileType.FAIL_ON_SUB_ARCHIVES, "true");
        runner.enqueue(archive);
        runner.run();
        single(DDArchiveContainsFileType.REL_FAILURE).assertAttributeExists(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY);
    }

    @Test
    void configFile(@TempDir final Path dir) throws IOException {
        final Path config = Files.writeString(dir.resolve("mimes.json"), "[\"text/plain\"]");
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_FILE, config.toString());
        runner.enqueue(zip("a.txt", utf8("hello")));
        runner.run();
        single(DDArchiveContainsFileType.REL_SUCCESS).assertAttributeEquals(DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, "true");
    }

    @Test
    void configValidation(@TempDir final Path dir) throws IOException {
        runner.assertNotValid();
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "not json");
        runner.assertNotValid();
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_BODY, "[\"text/plain\"]");
        runner.assertValid();
        runner.setProperty(DDArchiveContainsFileType.MIME_TYPES_CONFIG_FILE, Files.writeString(dir.resolve("m.json"), "[]").toString());
        runner.assertNotValid();
    }
}
