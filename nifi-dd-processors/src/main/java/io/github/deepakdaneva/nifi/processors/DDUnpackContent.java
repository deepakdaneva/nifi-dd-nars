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

import io.github.deepakdaneva.commons.archive.ArchiveUtil;
import io.github.deepakdaneva.commons.archive.NotAnArchiveOrSupportedArchiveException;
import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import io.github.deepakdaneva.nifi.core.DDCoreAttributes.DDFragmentAttributes;
import io.github.deepakdaneva.nifi.exceptions.NothingUnpackedException;
import io.github.deepakdaneva.nifi.exceptions.UnpackSizeThresholdReachedException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Unpacks an archive FlowFile into one FlowFile per contained file, optionally descending into nested archives.
 * <p>
 * Portions of this class are adapted from Apache NiFi's {@code UnpackContent} (Apache License 2.0).
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "Unpack", "un-merge", "tar", "zip", "archive", "7z", "recursive"})
@CapabilityDescription("Unpacks an archive FlowFile, emitting one FlowFile per contained file to 'success' and the original FlowFile to 'original'. " + "Supported formats are those of Apache Commons Compress (zip, jar, tar, 7z, ar, arj, cpio, dump and tar compressed with gzip, bzip2, xz, lzma, brotli, zstd, ...); " + "MIME types are detected with Apache Tika. The total number of unpacked bytes is limited by 'Unpack Size Threshold', counting the actual bytes " + "written rather than the sizes declared by the archive, and nested archives are opened up to 'Maximum Sub Archive Depth'. " + "Entry names are sanitized: '..' and '.' segments, leading separators and backslashes are removed from the 'filename' and 'path' attributes.")
@ReadsAttribute(attribute = DDCoreAttributes.FILENAME, description = "Used to populate '" + DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME + "' and '" + DDCoreAttributes.ABSOLUTE_PATH + "' of the unpacked FlowFiles.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.MIME_TYPE, description = "On the original FlowFile: the detected MIME type. On unpacked FlowFiles: the detected MIME type when 'Unpack Recursively' is true, otherwise 'application/octet-stream'."), @WritesAttribute(attribute = DDCoreAttributes.FILENAME, description = "Unpacked FlowFiles: file name of the entry, without directories."), @WritesAttribute(attribute = DDCoreAttributes.PATH, description = "Unpacked FlowFiles: directory of the entry inside its archive, ending with '/', or '/' for top level entries."), @WritesAttribute(attribute = DDCoreAttributes.ABSOLUTE_PATH, description = "Unpacked FlowFiles: the original FlowFile's filename followed by '/' and the entry directory (without trailing '/')."), @WritesAttribute(attribute = DDFragmentAttributes.FRAGMENT_IDENTIFIER, description = "All unpacked FlowFiles produced from the same parent FlowFile, and the original FlowFile, share the same randomly generated UUID."), @WritesAttribute(attribute = DDFragmentAttributes.FRAGMENT_INDEX, description = "Unpacked FlowFiles: one-up number giving the order in which the entries were unpacked."), @WritesAttribute(attribute = DDFragmentAttributes.FRAGMENT_COUNT, description = "Number of unpacked FlowFiles generated from the parent FlowFile (also set on the original FlowFile)."), @WritesAttribute(attribute = DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME, description = "Unpacked FlowFiles: filename of the parent FlowFile."), @WritesAttribute(attribute = DDFragmentAttributes.SEGMENT_PATH, description = "Unpacked FlowFiles: same as 'path'."), @WritesAttribute(attribute = DDUnpackContent.FILE_LAST_MODIFIED_TIME_ATTRIBUTE, description = "Unpacked FlowFiles from tar archives: last modification time of the entry (yyyy-MM-dd'T'HH:mm:ssZ)."), @WritesAttribute(attribute = DDUnpackContent.FILE_CREATION_TIME_ATTRIBUTE, description = "Unpacked FlowFiles from tar archives: same value as file.lastModifiedTime."), @WritesAttribute(attribute = DDUnpackContent.FILE_OWNER_ATTRIBUTE, description = "Unpacked FlowFiles from tar archives: owner of the entry."), @WritesAttribute(attribute = DDUnpackContent.FILE_GROUP_ATTRIBUTE, description = "Unpacked FlowFiles from tar archives: group of the entry."), @WritesAttribute(attribute = DDUnpackContent.FILE_PERMISSIONS_ATTRIBUTE, description = "Unpacked FlowFiles from tar archives: permissions of the entry, e.g. rw-r--r--."), @WritesAttribute(attribute = DDUnpackContent.UNPACK_SIZE_THRESHOLD_BREACHED, description = "Original FlowFile: 'true' if the archive held more data than 'Unpack Size Threshold' allowed."), @WritesAttribute(attribute = DDUnpackContent.UNPACKED_BYTES_ATTRIBUTE, description = "Original FlowFile: total number of bytes unpacked."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
@SystemResourceConsideration(resource = SystemResource.CPU, description = "When 'Unpack Recursively' is true every entry is inspected to detect its MIME type.")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Unpacked FlowFiles are held in the session until the whole archive has been processed. 7z archives need random access and are buffered in memory.")
@SeeAlso({DDArchiveContainsFileType.class, DDIdentifyMimeType.class})
public class DDUnpackContent extends AbstractDDProcessor {

    /**
     * Last modification time of an unpacked entry.
     */
    public static final String FILE_LAST_MODIFIED_TIME_ATTRIBUTE = "file.lastModifiedTime";
    /**
     * Creation time of an unpacked entry (same as the last modification time).
     */
    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    /**
     * Owner of an unpacked entry.
     */
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    /**
     * Group of an unpacked entry.
     */
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    /**
     * Permissions of an unpacked entry.
     */
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    /**
     * Whether the unpack size threshold was reached.
     */
    public static final String UNPACK_SIZE_THRESHOLD_BREACHED = "unpack.size.threshold.breached";
    /**
     * Total number of bytes unpacked.
     */
    public static final String UNPACKED_BYTES_ATTRIBUTE = "unpack.bytes";
    /**
     * Format of the time attributes.
     */
    public static final String FILE_MODIFIED_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    /**
     * Descends into nested archives.
     */
    public static final PropertyDescriptor UNPACK_RECURSIVELY = new PropertyDescriptor.Builder().name("Unpack Recursively").description("If true, supported archives found inside the archive are unpacked recursively instead of being emitted as files.").required(true).allowableValues("true", "false").defaultValue("false").build();

    /**
     * Maximum amount of data to unpack.
     */
    public static final PropertyDescriptor UNPACK_SIZE_THRESHOLD = new PropertyDescriptor.Builder().name("Unpack Size Threshold").description("Maximum total amount of data that may be unpacked from one FlowFile, e.g. with 3 entries of 30 MB and a threshold of 80 MB only 2 entries can be unpacked. " + "The actual number of decompressed bytes is counted, so archives declaring false entry sizes cannot exceed it. Set -1 for no limit.").required(true).addValidator(DATA_SIZE_OR_UNLIMITED_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).defaultValue("80 MB").build();

    /**
     * Routes to failure when the threshold is reached.
     */
    public static final PropertyDescriptor FAILURE_ON_THRESHOLD_BREACH = new PropertyDescriptor.Builder().name("Failure on Threshold Breach").description("If true and the archive holds more data than 'Unpack Size Threshold', the FlowFile is routed to 'failure' and nothing is emitted. " + "If false, entries that fit within the threshold are unpacked and the rest are skipped; this may take longer because every entry is still visited.").required(true).allowableValues("true", "false").defaultValue("true").build();

    /**
     * Routes to failure when a nested archive cannot be unpacked.
     */
    public static final PropertyDescriptor FAILURE_ON_SUB_ARCHIVE_UNPACKING = new PropertyDescriptor.Builder().name("Failure on Sub-Archive Unpacking").description("If true and a nested archive cannot be unpacked, the FlowFile is routed to 'failure'. If false, the remainder of that nested archive is skipped.").required(true).allowableValues("true", "false").defaultValue("true").dependsOn(UNPACK_RECURSIVELY, "true").build();

    /**
     * Maximum nesting depth.
     */
    public static final PropertyDescriptor MAX_SUB_ARCHIVE_DEPTH = new PropertyDescriptor.Builder().name("Maximum Sub Archive Depth").description("Maximum nesting depth of archives that are unpacked when 'Unpack Recursively' is true. Archives nested deeper are emitted as files. Protects against recursive archive bombs.").required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("10").dependsOn(UNPACK_RECURSIVELY, "true").build();

    /**
     * Routes to failure when nothing is unpacked.
     */
    public static final PropertyDescriptor FAILURE_ON_NOTHING_UNPACKED = new PropertyDescriptor.Builder().name("Failure on Nothing Unpacked").description("If true and nothing is unpacked from an archive, the FlowFile is routed to 'failure'. If false, only the original FlowFile is transferred, to 'original'.").required(true).allowableValues("true", "false").defaultValue("true").build();

    /**
     * Routes non-archive or unsupported FlowFiles to failure.
     */
    public static final PropertyDescriptor FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES = new PropertyDescriptor.Builder().name("Non Archive/Unsupported to Failure").description("If true, FlowFiles that are not archives or are unsupported archives are routed to 'failure'; otherwise they are routed to 'original'.").required(true).allowableValues("true", "false").defaultValue("false").build();

    /**
     * Relationship for unpacked FlowFiles.
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Unpacked FlowFiles.").build();
    /**
     * Relationship for the original FlowFile.
     */
    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original").description("The original FlowFile after it has been unpacked, or when it is not an archive and 'Non Archive/Unsupported to Failure' is false.").build();
    /**
     * Relationship for FlowFiles that cannot be unpacked.
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("The original FlowFile when it cannot be unpacked; no unpacked FlowFiles are emitted.").build();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFIED_DATE_ATTR_FORMAT).withZone(ZoneId.systemDefault());
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(UNPACK_RECURSIVELY, UNPACK_SIZE_THRESHOLD, FAILURE_ON_THRESHOLD_BREACH, FAILURE_ON_SUB_ARCHIVE_UNPACKING, MAX_SUB_ARCHIVE_DEPTH, FAILURE_ON_NOTHING_UNPACKED, FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL);
    private static final int COPY_BUFFER_SIZE = 64 * 1024;

    private volatile Detector detector;
    private volatile boolean recurse;
    private volatile long sizeThreshold;
    private volatile boolean failOnSizeBreach;
    private volatile boolean failOnSubArchiveError;
    private volatile int maxDepth;
    private volatile boolean failOnNothingUnpacked;
    private volatile boolean failOnUnsupported;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Loads the configuration when the processor is scheduled.
     *
     * @param context process context.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        detector = TikaConfig.getDefaultConfig().getDetector();
        recurse = context.getProperty(UNPACK_RECURSIVELY).asBoolean();
        sizeThreshold = parseDataSizeOrUnlimited(context.getProperty(UNPACK_SIZE_THRESHOLD));
        failOnSizeBreach = context.getProperty(FAILURE_ON_THRESHOLD_BREACH).asBoolean();
        // dependent properties have no value when recursion is disabled
        failOnSubArchiveError = recurse && context.getProperty(FAILURE_ON_SUB_ARCHIVE_UNPACKING).asBoolean();
        maxDepth = recurse ? context.getProperty(MAX_SUB_ARCHIVE_DEPTH).asInteger() : 1;
        failOnNothingUnpacked = context.getProperty(FAILURE_ON_NOTHING_UNPACKED).asBoolean();
        failOnUnsupported = context.getProperty(FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final UnpackState state = new UnpackState(session, original);
        Exception failure = null;
        try {
            unpackFlowFile(state);
            if (state.children.isEmpty() && failOnNothingUnpacked) {
                failure = new NothingUnpackedException("Nothing unpacked from the archive");
            }
        } catch (final Exception e) {
            failure = e;
        }

        final Map<String, String> attributes = new HashMap<>(exceptionAttributes(failure));
        attributes.put(UNPACK_SIZE_THRESHOLD_BREACHED, String.valueOf(state.thresholdReached));
        attributes.put(UNPACKED_BYTES_ATTRIBUTE, String.valueOf(state.bytes));
        if (state.mimeType != null) {
            attributes.put(DDCoreAttributes.MIME_TYPE, state.mimeType);
        }

        if (failure == null) {
            if (!state.children.isEmpty()) {
                attributes.put(DDFragmentAttributes.FRAGMENT_IDENTIFIER, state.fragmentId);
                attributes.put(DDFragmentAttributes.FRAGMENT_COUNT, String.valueOf(state.children.size()));
                final List<FlowFile> children = finishChildren(session, state);
                session.getProvenanceReporter().fork(original, children);
                session.transfer(children, REL_SUCCESS);
            }
            original = session.putAllAttributes(original, attributes);
            session.transfer(original, REL_ORIGINAL);
            return;
        }

        session.remove(state.children);
        original = session.putAllAttributes(original, attributes);
        session.getProvenanceReporter().modifyAttributes(original);
        if (failure instanceof NotAnArchiveOrSupportedArchiveException && !failOnUnsupported) {
            getLogger().debug("{} is not a supported archive; routing to original", original);
            session.transfer(original, REL_ORIGINAL);
        } else {
            if (failure instanceof NothingUnpackedException || failure instanceof UnpackSizeThresholdReachedException || failure instanceof NotAnArchiveOrSupportedArchiveException) {
                getLogger().warn("Failed to unpack {}: {}", original, failure.getMessage());
            } else {
                getLogger().error("Failed to unpack {}", original, failure);
            }
            session.transfer(original, REL_FAILURE);
        }
    }

    private void unpackFlowFile(final UnpackState state) throws IOException {
        try (InputStream in = TikaInputStream.get(new BufferedInputStream(state.session.read(state.original)))) {
            state.mimeType = detectMimeType(detector, in);
            try (ArchiveInputStream<?> archive = openTopLevel(in, state.mimeType)) {
                unpack(state, archive, 1);
            }
        }
    }

    private static ArchiveInputStream<?> openTopLevel(final InputStream in, final String mimeType) throws IOException {
        try {
            return ArchiveUtil.getArchiveInputStream(in, mimeType);
        } catch (final IOException | RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException("Failed to open archive of type " + mimeType, e);
        }
    }

    private void unpack(final UnpackState state, final ArchiveInputStream<?> archive, final int depth) throws IOException {
        ArchiveWalker.walk(archive, (entry, content) -> {
            final long declaredSize = entry.getSize();
            if (sizeThreshold != NO_LIMIT && declaredSize > 0 && state.bytes + declaredSize > sizeThreshold) {
                thresholdReached(state);
                return true;
            }
            String mimeType = OCTET_STREAM;
            InputStream leaf = content;
            if (recurse) {
                mimeType = detectMimeType(detector, content);
                if (depth < maxDepth) {
                    leaf = unpackNested(state, content, mimeType, depth);
                    if (leaf == null) {
                        return true;
                    }
                }
            }
            emit(state, entry, mimeType, leaf);
            return true;
        });
    }

    /**
     * Unpacks a nested archive.
     *
     * @return {@code null} if the entry was handled as an archive, otherwise the stream to emit as a file.
     */
    private InputStream unpackNested(final UnpackState state, final InputStream content, final String mimeType, final int depth) throws IOException {
        final ArchiveWalker.Nested nested;
        try {
            nested = ArchiveWalker.openNested(content, mimeType, sizeThreshold == NO_LIMIT ? NO_LIMIT : sizeThreshold - state.bytes);
        } catch (final ArchiveWalker.NestedArchiveTooLargeException e) {
            thresholdReached(state);
            return null;
        }
        if (nested.archive() == null) {
            return nested.content();
        }
        try (ArchiveInputStream<?> archive = nested.archive()) {
            unpack(state, archive, depth + 1);
        } catch (final UnpackSizeThresholdReachedException e) {
            throw e;
        } catch (final IOException | RuntimeException e) {
            if (failOnSubArchiveError) {
                throw e;
            }
            getLogger().warn("Skipping unreadable nested archive of type {} in {}", mimeType, state.original, e);
        }
        return null;
    }

    private void thresholdReached(final UnpackState state) {
        state.thresholdReached = true;
        if (failOnSizeBreach) {
            throw new UnpackSizeThresholdReachedException("Unpack threshold reached, archive has more data than the specified limit of " + sizeThreshold + " bytes");
        }
    }

    private void emit(final UnpackState state, final ArchiveEntry entry, final String mimeType, final InputStream content) {
        final long allowance = sizeThreshold == NO_LIMIT ? Long.MAX_VALUE : sizeThreshold - state.bytes;
        final long[] written = {0L};
        FlowFile child = state.session.create(state.original);
        try {
            child = state.session.write(child, out -> {
                final byte[] buffer = new byte[COPY_BUFFER_SIZE];
                int read;
                while ((read = content.read(buffer)) != -1) {
                    written[0] += read;
                    if (written[0] > allowance) {
                        return;
                    }
                    out.write(buffer, 0, read);
                }
            });
        } catch (final RuntimeException e) {
            state.session.remove(child);
            throw e;
        }
        if (written[0] > allowance) {
            state.session.remove(child);
            thresholdReached(state);
            return;
        }
        state.bytes += written[0];
        child = state.session.putAllAttributes(child, entryAttributes(state, entry, mimeType));
        state.children.add(child);
    }

    private static Map<String, String> entryAttributes(final UnpackState state, final ArchiveEntry entry, final String mimeType) {
        final EntryName name = EntryName.of(entry.getName(), state.children.size() + 1);
        final String parentFileName = state.original.getAttribute(DDCoreAttributes.FILENAME);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(DDCoreAttributes.FILENAME, name.fileName());
        attributes.put(DDCoreAttributes.PATH, name.path());
        attributes.put(DDCoreAttributes.ABSOLUTE_PATH, parentFileName + "/" + name.parentDirectory());
        attributes.put(DDCoreAttributes.MIME_TYPE, mimeType == null ? OCTET_STREAM : mimeType);
        attributes.put(DDFragmentAttributes.FRAGMENT_IDENTIFIER, state.fragmentId);
        attributes.put(DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME, parentFileName);
        attributes.put(DDFragmentAttributes.SEGMENT_PATH, name.path());
        if (entry instanceof TarArchiveEntry tarEntry) {
            attributes.put(FILE_PERMISSIONS_ATTRIBUTE, permissionToString(tarEntry.getMode()));
            attributes.put(FILE_OWNER_ATTRIBUTE, String.valueOf(tarEntry.getUserName()));
            attributes.put(FILE_GROUP_ATTRIBUTE, String.valueOf(tarEntry.getGroupName()));
            final Date modified = tarEntry.getLastModifiedDate();
            if (modified != null) {
                final String time = DATE_TIME_FORMATTER.format(modified.toInstant());
                attributes.put(FILE_LAST_MODIFIED_TIME_ATTRIBUTE, time);
                attributes.put(FILE_CREATION_TIME_ATTRIBUTE, time);
            }
        }
        return attributes;
    }

    private static List<FlowFile> finishChildren(final ProcessSession session, final UnpackState state) {
        final List<FlowFile> finished = new ArrayList<>(state.children.size());
        final String count = String.valueOf(state.children.size());
        for (int i = 0; i < state.children.size(); i++) {
            finished.add(session.putAllAttributes(state.children.get(i), Map.of(DDFragmentAttributes.FRAGMENT_COUNT, count, DDFragmentAttributes.FRAGMENT_INDEX, String.valueOf(i + 1))));
        }
        return finished;
    }

    /**
     * Converts Unix permission bits (as stored in a tar header, e.g. {@code 0100644}) to the {@code rwxrwxrwx} notation.
     * Only the lowest nine bits are considered.
     *
     * @param mode Unix mode bits.
     * @return permissions such as {@code rw-r--r--}.
     */
    public static String permissionToString(final int mode) {
        final char[] chars = "rwxrwxrwx".toCharArray();
        for (int bit = 0; bit < 9; bit++) {
            if ((mode & (1 << (8 - bit))) == 0) {
                chars[bit] = '-';
            }
        }
        return new String(chars);
    }

    /**
     * Sanitized name of an archive entry.
     *
     * @param fileName file name without directories.
     * @param directory directory of the entry ending with '/', or empty for top level entries.
     */
    record EntryName(String fileName, String directory) {

        /**
         * @return the directory, or '/' for top level entries.
         */
        String path() {
            return directory.isEmpty() ? "/" : directory;
        }

        /**
         * @return the directory without the trailing '/', or empty for top level entries.
         */
        String parentDirectory() {
            return directory.isEmpty() ? "" : directory.substring(0, directory.length() - 1);
        }

        /**
         * Splits an entry name into directory and file name, dropping empty, '.' and '..' segments so the attributes
         * can never point outside the extraction directory.
         *
         * @param rawName name stored in the archive.
         * @param index one-up index used to name entries whose name is empty after sanitizing.
         * @return sanitized name.
         */
        static EntryName of(final String rawName, final int index) {
            final List<String> segments = new ArrayList<>();
            for (final String segment : (rawName == null ? "" : rawName).replace('\\', '/').split("/")) {
                if (!segment.isEmpty() && !".".equals(segment) && !"..".equals(segment)) {
                    segments.add(segment);
                }
            }
            if (segments.isEmpty()) {
                return new EntryName("unnamed-entry-" + index, "");
            }
            final String fileName = segments.removeLast();
            return new EntryName(fileName, segments.isEmpty() ? "" : String.join("/", segments) + "/");
        }
    }

    /**
     * Mutable state of one onTrigger invocation.
     */
    private static final class UnpackState {
        private final ProcessSession session;
        private final FlowFile original;
        private final String fragmentId = UUID.randomUUID().toString();
        private final List<FlowFile> children = new ArrayList<>();
        private String mimeType;
        private long bytes;
        private boolean thresholdReached;

        private UnpackState(final ProcessSession session, final FlowFile original) {
            this.session = session;
            this.original = original;
        }
    }
}
