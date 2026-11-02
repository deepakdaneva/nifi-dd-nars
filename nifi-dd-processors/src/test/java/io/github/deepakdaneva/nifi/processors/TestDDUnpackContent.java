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
import io.github.deepakdaneva.nifi.core.DDCoreAttributes.DDFragmentAttributes;
import io.github.deepakdaneva.nifi.exceptions.NothingUnpackedException;
import io.github.deepakdaneva.nifi.exceptions.UnpackSizeThresholdReachedException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.deepakdaneva.nifi.TestArchives.entries;
import static io.github.deepakdaneva.nifi.TestArchives.sevenZ;
import static io.github.deepakdaneva.nifi.TestArchives.tar;
import static io.github.deepakdaneva.nifi.TestArchives.utf8;
import static io.github.deepakdaneva.nifi.TestArchives.zip;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link DDUnpackContent}.
 */
class TestDDUnpackContent {

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new DDUnpackContent());
    }

    private void enqueue(final byte[] content, final String filename) {
        runner.enqueue(content, Map.of(DDCoreAttributes.FILENAME, filename));
    }

    @Test
    void recursiveUnpack() throws IOException {
        runner.setProperty(DDUnpackContent.UNPACK_RECURSIVELY, "true");
        runner.setProperty(DDUnpackContent.FAILURE_ON_SUB_ARCHIVE_UNPACKING, "true");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("zip_inside_zip"));
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(DDUnpackContent.REL_ORIGINAL, 1);
        final List<MockFlowFile> children = runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS);
        children.forEach(ff -> {
            ff.assertAttributeEquals(DDFragmentAttributes.FRAGMENT_COUNT, "2");
            ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "text/plain");
            ff.assertContentEquals("Hello World");
        });
        final MockFlowFile original = runner.getFlowFilesForRelationship(DDUnpackContent.REL_ORIGINAL).get(0);
        final String fragmentId = original.getAttribute(DDFragmentAttributes.FRAGMENT_IDENTIFIER);
        children.forEach(ff -> ff.assertAttributeEquals(DDFragmentAttributes.FRAGMENT_IDENTIFIER, fragmentId));
        original.assertAttributeEquals(DDFragmentAttributes.FRAGMENT_COUNT, "2");
        original.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/zip");
        original.assertAttributeEquals(DDUnpackContent.UNPACKED_BYTES_ATTRIBUTE, "22");
        original.assertAttributeEquals(DDUnpackContent.UNPACK_SIZE_THRESHOLD_BREACHED, "false");
        assertEquals(Set.of("1", "2"), children.stream().map(ff -> ff.getAttribute(DDFragmentAttributes.FRAGMENT_INDEX)).collect(Collectors.toSet()));
    }

    @Test
    void nonRecursiveUnpackEmitsNestedArchiveAsFile() throws IOException {
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("zip_inside_zip"));
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 2);
        final Set<String> names = runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).stream().map(ff -> ff.getAttribute(DDCoreAttributes.FILENAME)).collect(Collectors.toSet());
        assertEquals(Set.of("txt_file", "txt_file.zip"), names);
        runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).forEach(ff -> ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/octet-stream"));
    }

    @Test
    void entryNamesAreSanitized() {
        enqueue(zip(entries("../../etc/passwd", utf8("a"), "/abs/dir/file.txt", utf8("b"), "win\\dir\\x.txt", utf8("c"), "./", utf8(""))), "in.zip");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 3);
        final Map<String, MockFlowFile> byName = runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).stream().collect(Collectors.toMap(ff -> ff.getAttribute(DDCoreAttributes.FILENAME), ff -> ff));
        byName.get("passwd").assertAttributeEquals(DDCoreAttributes.PATH, "etc/");
        byName.get("passwd").assertAttributeEquals(DDCoreAttributes.ABSOLUTE_PATH, "in.zip/etc");
        byName.get("file.txt").assertAttributeEquals(DDCoreAttributes.PATH, "abs/dir/");
        byName.get("x.txt").assertAttributeEquals(DDCoreAttributes.PATH, "win/dir/");
        byName.get("x.txt").assertAttributeEquals(DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME, "in.zip");
    }

    @Test
    void tarAttributes() {
        enqueue(tar(entries("dir/a.txt", utf8("hello")), 0100640, true), "in.tgz");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).get(0);
        ff.assertContentEquals("hello");
        ff.assertAttributeEquals(DDCoreAttributes.FILENAME, "a.txt");
        ff.assertAttributeEquals(DDCoreAttributes.PATH, "dir/");
        ff.assertAttributeEquals(DDUnpackContent.FILE_PERMISSIONS_ATTRIBUTE, "rw-r-----");
        ff.assertAttributeEquals(DDUnpackContent.FILE_OWNER_ATTRIBUTE, "alice");
        ff.assertAttributeEquals(DDUnpackContent.FILE_GROUP_ATTRIBUTE, "staff");
        ff.assertAttributeExists(DDUnpackContent.FILE_LAST_MODIFIED_TIME_ATTRIBUTE);
        ff.assertAttributeExists(DDUnpackContent.FILE_CREATION_TIME_ATTRIBUTE);
    }

    @Test
    void sevenZipTopLevelAndNested() {
        final byte[] inner = sevenZ(entries("inner.txt", utf8("inner")));
        runner.setProperty(DDUnpackContent.UNPACK_RECURSIVELY, "true");
        enqueue(sevenZ(entries("outer.txt", utf8("outer"), "inner.7z", inner)), "in.7z");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 2);
        final Set<String> contents = runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).stream().map(ff -> new String(ff.toByteArray())).collect(Collectors.toSet());
        assertEquals(Set.of("outer", "inner"), contents);
    }

    @Test
    void thresholdCountsActualBytesNotDeclaredSize() {
        // streaming zip entries declare no size, so only counting the decompressed bytes can stop them
        runner.setProperty(DDUnpackContent.UNPACK_SIZE_THRESHOLD, "1 KB");
        enqueue(zip("zeros.bin", new byte[1024 * 1024]), "bomb.zip");
        runner.run();
        runner.assertAllFlowFilesTransferred(DDUnpackContent.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDUnpackContent.REL_FAILURE).get(0);
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, UnpackSizeThresholdReachedException.class.getName());
        ff.assertAttributeEquals(DDUnpackContent.UNPACK_SIZE_THRESHOLD_BREACHED, "true");
    }

    @Test
    void thresholdSkipsEntriesWhenNotFailing() {
        runner.setProperty(DDUnpackContent.UNPACK_SIZE_THRESHOLD, "10 B");
        runner.setProperty(DDUnpackContent.FAILURE_ON_THRESHOLD_BREACH, "false");
        enqueue(zip(entries("big.txt", utf8("0123456789ABCDEF"), "small.txt", utf8("ok"))), "in.zip");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(DDUnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).get(0).assertContentEquals("ok");
        final MockFlowFile original = runner.getFlowFilesForRelationship(DDUnpackContent.REL_ORIGINAL).get(0);
        original.assertAttributeEquals(DDUnpackContent.UNPACK_SIZE_THRESHOLD_BREACHED, "true");
        original.assertAttributeEquals(DDUnpackContent.UNPACKED_BYTES_ATTRIBUTE, "2");
    }

    @Test
    void unlimitedThreshold() {
        runner.setProperty(DDUnpackContent.UNPACK_SIZE_THRESHOLD, "-1");
        enqueue(zip("a.txt", utf8("a")), "in.zip");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 1);
    }

    @Test
    void invalidThreshold() {
        runner.setProperty(DDUnpackContent.UNPACK_SIZE_THRESHOLD, "lots");
        runner.assertNotValid();
    }

    @Test
    void maximumDepthEmitsDeeperArchivesAsFiles() {
        final byte[] level3 = zip("deep.txt", utf8("deep"));
        final byte[] level2 = zip("level3.zip", level3);
        runner.setProperty(DDUnpackContent.UNPACK_RECURSIVELY, "true");
        runner.setProperty(DDUnpackContent.MAX_SUB_ARCHIVE_DEPTH, "2");
        enqueue(zip("level2.zip", level2), "level1.zip");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_SUCCESS, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDUnpackContent.REL_SUCCESS).get(0);
        ff.assertAttributeEquals(DDCoreAttributes.FILENAME, "level3.zip");
        ff.assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "application/zip");
    }

    @Test
    void corruptSubArchiveFailsOrIsSkipped() {
        final byte[] corrupt = utf8("PK\u0003\u0004 this is not really a zip");
        final byte[] archive = zip(entries("ok.txt", utf8("ok"), "bad.zip", corrupt));

        runner.setProperty(DDUnpackContent.UNPACK_RECURSIVELY, "true");
        runner.setProperty(DDUnpackContent.FAILURE_ON_SUB_ARCHIVE_UNPACKING, "false");
        enqueue(archive, "in.zip");
        runner.run();
        runner.assertTransferCount(DDUnpackContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(DDUnpackContent.REL_FAILURE, 0);

        runner.clearTransferState();
        runner.setProperty(DDUnpackContent.FAILURE_ON_SUB_ARCHIVE_UNPACKING, "true");
        enqueue(archive, "in.zip");
        runner.run();
        runner.assertAllFlowFilesTransferred(DDUnpackContent.REL_FAILURE, 1);
    }

    @Test
    void nonArchiveRoutedToOriginalOrFailure() throws IOException {
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("txt_file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(DDUnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(DDUnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(DDCoreAttributes.MIME_TYPE, "text/plain");

        runner.clearTransferState();
        runner.setProperty(DDUnpackContent.FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES, "true");
        runner.enqueue(DDTestConstants.TEST_FILES_PATH.resolve("txt_file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(DDUnpackContent.REL_FAILURE, 1);
    }

    @Test
    void emptyArchive() {
        final byte[] empty = zip(Map.of());
        enqueue(empty, "empty.zip");
        runner.run();
        runner.assertAllFlowFilesTransferred(DDUnpackContent.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(DDUnpackContent.REL_FAILURE).get(0).assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, NothingUnpackedException.class.getName());

        runner.clearTransferState();
        runner.setProperty(DDUnpackContent.FAILURE_ON_NOTHING_UNPACKED, "false");
        enqueue(empty, "empty.zip");
        runner.run();
        runner.assertAllFlowFilesTransferred(DDUnpackContent.REL_ORIGINAL, 1);
    }

    @Test
    void permissionToString() {
        assertEquals("rwxr-xr-x", DDUnpackContent.permissionToString(0100755));
        assertEquals("rw-r--r--", DDUnpackContent.permissionToString(0644));
        assertEquals("---------", DDUnpackContent.permissionToString(0));
    }

    @Test
    void entryNameWithoutSegments() {
        final DDUnpackContent.EntryName name = DDUnpackContent.EntryName.of("/../", 3);
        assertEquals("unnamed-entry-3", name.fileName());
        assertEquals("/", name.path());
    }
}
