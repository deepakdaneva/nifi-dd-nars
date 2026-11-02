/*
 * Copyright (C) 2026 Deepak Kumar Jangir
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
package io.github.deepakdaneva.nifi;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds small archives in memory for tests.
 */
public final class TestArchives {

    private TestArchives() {
    }

    /**
     * @param text text.
     * @return UTF-8 bytes.
     */
    public static byte[] utf8(final String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * @param entries name to content, in insertion order.
     * @return a zip archive written in streaming mode, so entry sizes are not known before reading the data.
     */
    public static byte[] zip(final Map<String, byte[]> entries) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        // wrapping in a plain OutputStream forces data descriptors (unknown sizes in local headers)
        try (ZipArchiveOutputStream zip = new ZipArchiveOutputStream(new OutputStream() {
            @Override
            public void write(final int b) {
                bytes.write(b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) {
                bytes.write(b, off, len);
            }
        })) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                zip.putArchiveEntry(new ZipArchiveEntry(entry.getKey()));
                zip.write(entry.getValue());
                zip.closeArchiveEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /**
     * @param name entry name.
     * @param content entry content.
     * @return a zip archive with one entry.
     */
    public static byte[] zip(final String name, final byte[] content) {
        return zip(Map.of(name, content));
    }

    /**
     * @param entries name to content.
     * @param mode Unix mode of every entry.
     * @param gzip whether to gzip the tar.
     * @return a tar (or tar.gz) archive.
     */
    public static byte[] tar(final Map<String, byte[]> entries, final int mode, final boolean gzip) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream target = gzip ? new GzipCompressorOutputStream(bytes) : bytes; TarArchiveOutputStream tar = new TarArchiveOutputStream(target)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                final TarArchiveEntry tarEntry = new TarArchiveEntry(entry.getKey());
                tarEntry.setSize(entry.getValue().length);
                tarEntry.setMode(mode);
                tarEntry.setUserName("alice");
                tarEntry.setGroupName("staff");
                tarEntry.setModTime(new Date(0));
                tar.putArchiveEntry(tarEntry);
                tar.write(entry.getValue());
                tar.closeArchiveEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /**
     * @param entries name to content.
     * @return a 7z archive.
     */
    public static byte[] sevenZ(final Map<String, byte[]> entries) {
        final SeekableInMemoryByteChannel channel = new SeekableInMemoryByteChannel();
        try (SevenZOutputFile out = new SevenZOutputFile(channel)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                final SevenZArchiveEntry archiveEntry = new SevenZArchiveEntry();
                archiveEntry.setName(entry.getKey());
                archiveEntry.setSize(entry.getValue().length);
                out.putArchiveEntry(archiveEntry);
                out.write(entry.getValue());
                out.closeArchiveEntry();
            }
            out.finish();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Arrays.copyOf(channel.array(), (int) channel.size());
    }

    /**
     * @param keyValues alternating names and contents.
     * @return ordered map.
     */
    public static Map<String, byte[]> entries(final Object... keyValues) {
        final Map<String, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], (byte[]) keyValues[i + 1]);
        }
        return map;
    }
}
