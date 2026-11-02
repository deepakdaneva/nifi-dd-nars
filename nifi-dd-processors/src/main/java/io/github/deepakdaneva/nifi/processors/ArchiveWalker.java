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
package io.github.deepakdaneva.nifi.processors;

import io.github.deepakdaneva.commons.archive.ArchiveUtil;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.tika.io.TikaInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Streaming traversal helpers for archives opened through {@link ArchiveUtil}.
 * <p>
 * Entries are streamed straight from the archive, so memory use does not grow with entry size. The only exception is
 * a nested 7z archive, which needs random access and is therefore buffered in memory up to a caller supplied limit.
 */
final class ArchiveWalker {

    /**
     * Read-ahead allowed while probing an entry for a nested archive; the stream is reset to the entry start afterwards.
     */
    static final int PROBE_MARK_LIMIT = 1024 * 1024;

    private static final int BUFFER_SIZE = 64 * 1024;

    private ArchiveWalker() {
    }

    /**
     * Receives the entries of an archive.
     */
    @FunctionalInterface
    interface EntryVisitor {
        /**
         * Called for every non-directory entry.
         *
         * @param entry archive entry.
         * @param content entry content as a {@link TikaInputStream}; supports mark/reset and is closed after the
         * visit. Closing it does not close the archive.
         * @return {@code true} to continue with the next entry, {@code false} to stop the traversal.
         * @throws IOException if the entry cannot be read.
         */
        boolean visit(ArchiveEntry entry, InputStream content) throws IOException;
    }

    /**
     * A nested archive opened from an entry, or the entry content when it turned out not to be an archive.
     *
     * @param archive opened nested archive, or {@code null}.
     * @param content stream to use as the entry content when {@code archive} is {@code null}.
     */
    record Nested(ArchiveInputStream<?> archive, InputStream content) {
    }

    /**
     * Iterates all non-directory entries of an archive. The archive stream is not closed.
     *
     * @param archive archive to iterate.
     * @param visitor entry visitor.
     * @return {@code false} if the visitor stopped the traversal, otherwise {@code true}.
     * @throws IOException if the archive cannot be read.
     */
    static boolean walk(final ArchiveInputStream<?> archive, final EntryVisitor visitor) throws IOException {
        ArchiveEntry entry;
        while ((entry = archive.getNextEntry()) != null) {
            if (entry.isDirectory()) {
                continue;
            }
            try (InputStream content = TikaInputStream.get(new BufferedInputStream(CloseShieldInputStream.wrap(archive), BUFFER_SIZE))) {
                if (!visitor.visit(entry, content)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Tries to open the content of an entry as a nested archive.
     *
     * @param content entry content supporting mark/reset, positioned at the start of the entry.
     * @param mimeType detected MIME type of the entry.
     * @param maxBufferedBytes maximum number of bytes that may be buffered in memory for archives needing random access
     * (7z); {@link AbstractDDProcessor#NO_LIMIT} for no limit.
     * @return the nested archive, or the (repositioned) content if the entry is not a supported archive.
     * @throws IOException if reading fails.
     * @throws NestedArchiveTooLargeException if a 7z entry is larger than {@code maxBufferedBytes}.
     */
    static Nested openNested(final InputStream content, final String mimeType, final long maxBufferedBytes) throws IOException {
        if (!ArchiveUtil.isAnyArchiveMimeType(mimeType)) {
            return new Nested(null, content);
        }
        if (ArchiveUtil.is7zMimeType(mimeType)) {
            final byte[] bytes = readBounded(content, maxBufferedBytes);
            final ArchiveInputStream<?> archive = open(new ByteArrayInputStream(bytes), mimeType);
            return new Nested(archive, archive == null ? new ByteArrayInputStream(bytes) : null);
        }
        content.mark(PROBE_MARK_LIMIT);
        final ArchiveInputStream<?> archive = open(content, mimeType);
        if (archive == null) {
            content.reset();
        }
        return new Nested(archive, archive == null ? content : null);
    }

    private static ArchiveInputStream<?> open(final InputStream content, final String mimeType) throws IOException {
        try {
            return ArchiveUtil.getArchiveInputStream(content, mimeType, true);
        } catch (CompressorException | ArchiveException e) {
            return null;
        }
    }

    private static byte[] readBounded(final InputStream content, final long maxBytes) throws IOException {
        final long limit = maxBytes < 0 ? Integer.MAX_VALUE - 8L : Math.min(maxBytes, Integer.MAX_VALUE - 8L);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] buffer = new byte[BUFFER_SIZE];
        long total = 0;
        int read;
        while ((read = content.read(buffer)) != -1) {
            total += read;
            if (total > limit) {
                throw new NestedArchiveTooLargeException("Nested 7z archive exceeds " + limit + " bytes and cannot be buffered");
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    /**
     * Raised when a nested archive needing random access is larger than the allowed in-memory buffer.
     */
    static final class NestedArchiveTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;

        NestedArchiveTooLargeException(final String message) {
            super(message);
        }
    }
}
