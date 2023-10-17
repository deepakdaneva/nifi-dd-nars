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
import io.github.deepakdaneva.commons.archive.ArchiveUtil;
import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import io.github.deepakdaneva.nifi.exceptions.NothingUnpackedException;
import io.github.deepakdaneva.nifi.exceptions.UnpackSizeThresholdReachedException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "Unpack", "un-merge", "tar", "zip", "archive", "7z"})
@CapabilityDescription("Unpacks the content of given archive FlowFile, emitting one to many FlowFiles for each input FlowFile. NOTE: This processor is backed by Apache Tika (https://tika.apache.org) and Apache Commons Compress (https://commons.apache.org/proper/commons-compress) check these to know more.")
@ReadsAttribute(attribute = DDCoreAttributes.FILENAME, description = "To generate '" + DDCoreAttributes.DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME + "' and '" + DDCoreAttributes.DDFragmentAttributes.SEGMENT_PATH + "' this processor will read '" + DDCoreAttributes.FILENAME + "'.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.MIME_TYPE, description = "This processor will set the detected mime.type of the original FlowFile and If 'Unpack Recursively' is set to true then this processor will use 'Apache Tika' to identify the mime type of the unpacked file in this case and '" + DDCoreAttributes.MIME_TYPE + "' will be set for all unpacked files else this attribute is set to application/octet-stream."), @WritesAttribute(attribute = DDCoreAttributes.DDFragmentAttributes.FRAGMENT_IDENTIFIER, description = "All unpacked FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"), @WritesAttribute(attribute = DDCoreAttributes.DDFragmentAttributes.FRAGMENT_INDEX, description = "A one-up number that indicates the ordering of the unpacked FlowFiles that were created from a single parent FlowFile"), @WritesAttribute(attribute = DDCoreAttributes.DDFragmentAttributes.FRAGMENT_COUNT, description = "The number of unpacked FlowFiles generated from the parent FlowFile"), @WritesAttribute(attribute = DDCoreAttributes.DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME, description = "The filename of the parent FlowFile."), @WritesAttribute(attribute = DDCoreAttributes.DDFragmentAttributes.SEGMENT_PATH, description = "The path of the FlowFile relative to the parent FlowFile."), @WritesAttribute(attribute = DDUnpackContent.FILE_LAST_MODIFIED_TIME_ATTRIBUTE, description = "The date and time that the unpacked file was last modified (if archive file supports it)."), @WritesAttribute(attribute = DDUnpackContent.FILE_CREATION_TIME_ATTRIBUTE, description = "The date and time that the file was created. This attribute holds always the same value as file.lastModifiedTime (if archive file supports it)."), @WritesAttribute(attribute = DDUnpackContent.FILE_OWNER_ATTRIBUTE, description = "The owner of the unpacked file (if archive file supports it)."), @WritesAttribute(attribute = DDUnpackContent.FILE_GROUP_ATTRIBUTE, description = "The group owner of the unpacked file (if archive file supports it)."), @WritesAttribute(attribute = DDUnpackContent.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the unpacked file (if archive file supports it)."), @WritesAttribute(attribute = DDUnpackContent.UNPACK_SIZE_THRESHOLD_BREACHED, description = "If unpack size limit is reached and no more files can be unpacked from the archive then this attribute will be set for the file routed to original."), @WritesAttribute(attribute = DDUnpackContent.UNPACKED_BYTES_ATTRIBUTE, description = "Total bytes unpacked from the archive will be provided through this attribute for the original FlowFile."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
@SystemResourceConsiderations({@SystemResourceConsideration(resource = SystemResource.CPU, description = "When 'Unpack Recursively' is true then it'll consume CPU for processing as it'll try to detect each file inside the archive for archive type and will process them recursively."), @SystemResourceConsideration(resource = SystemResource.MEMORY, description = "This processor will consume memory as it keeps the FlowFile references until those are transferred or removed.")})
public class DDUnpackContent extends AbstractProcessor {
    public static final String FILE_LAST_MODIFIED_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String UNPACK_SIZE_THRESHOLD_BREACHED = "unpack.size.threshold.breached";
    public static final String FILE_MODIFIED_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    public static final String UNPACKED_BYTES_ATTRIBUTE = "unpack.bytes";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFIED_DATE_ATTR_FORMAT).withZone(ZoneId.systemDefault());
    public static final String OCTET_STREAM = "application/octet-stream";
    public static final String SEMICOLON = ";";
    public static final PropertyDescriptor UNPACK_RECURSIVELY = new PropertyDescriptor.Builder().name("Unpack Recursively").description("If true will unpack all the supported sub archive files recursively.").required(true).allowableValues("true", "false").defaultValue("false").build();

    public static final PropertyDescriptor UNPACK_SIZE_THRESHOLD = new PropertyDescriptor.Builder().name("Unpack Size Threshold").description("Specifies the maximum amount of data that can be unpacked from the file. Let's say there are 3 files in the archive of size 30 MB each and this property is set to 80 MB then only 2 files can be unpacked from the archive. Set -1 for no limit.").required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("80 MB").build();

    public static final PropertyDescriptor FAILURE_ON_THRESHOLD_BREACH = new PropertyDescriptor.Builder().name("Failure on Threshold Breach").description("This simply means if archive has more data than the specified limit and If this is true then file will be routed to failure without unpacking any files. If false then only files which can be unpacked under the threshold limit will be unpacked and transferred without any error. NOTE: If false then it might take more time to iterate over all the file inside the archive and to calculate the entry size for possible unpack.").required(true).allowableValues("true", "false").defaultValue("true").build();

    public static final PropertyDescriptor FAILURE_ON_SUB_ARCHIVE_UNPACKING = new PropertyDescriptor.Builder().name("Failure on Sub-Archive Unpacking").description("If true then while unpacking the sub-archive files if any error occurred then the FlowFile will be routed to failure. If false then sub-archive will be skipped if any error occurred during the that sub-archive file unpacking.").required(true).allowableValues("true", "false").defaultValue("true").dependsOn(UNPACK_RECURSIVELY, "true").build();

    public static final PropertyDescriptor FAILURE_ON_NOTHING_UNPACKED = new PropertyDescriptor.Builder().name("Failure on Nothing Unpacked").description("If true and file is an archive then file will be routed to failure if nothing is unpacked from the FlowFile or given 'File Path', if false then nothing will be transferred to success only original file will be transferred.").required(true).allowableValues("true", "false").defaultValue("true").build();

    public static final PropertyDescriptor FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES = new PropertyDescriptor.Builder().name("Non Archive/Unsupported to Failure").description("If true will route all incoming non archive or unsupported archive FlowFile/File to failure.").required(true).allowableValues("true", "false").defaultValue("false").build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Unpacked FlowFiles are sent to this relationship").build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original").description("The original FlowFile is sent to this relationship after it has been successfully unpacked").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("The original FlowFile is sent to this relationship when it cannot be unpacked for some reason").build();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private Detector detector;
    private ComponentLog log;
    private boolean RECURSE_UNPACK;
    private long SIZE_THRESHOLD;
    private boolean FAIL_ON_SIZE_BREACH;
    private boolean FAIL_ON_SUB_ARCHIVE_FAILURE;
    private boolean FAIL_ON_NOTHING_UNPACKED;
    private boolean FAIL_ON_UNSUPPORTED;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        properties = List.of(UNPACK_RECURSIVELY, UNPACK_SIZE_THRESHOLD, FAILURE_ON_THRESHOLD_BREACH, FAILURE_ON_SUB_ARCHIVE_UNPACKING, FAILURE_ON_NOTHING_UNPACKED, FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL);
        detector = TikaConfig.getDefaultConfig().getDetector();
        log = getLogger();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        RECURSE_UNPACK = context.getProperty(UNPACK_RECURSIVELY).asBoolean();
        PropertyValue pv = context.getProperty(UNPACK_SIZE_THRESHOLD).evaluateAttributeExpressions();
        if (pv.getValue() != null && pv.getValue().trim().equals("-1")) {
            SIZE_THRESHOLD = -1L;
        } else {
            SIZE_THRESHOLD = pv.asDataSize(DataUnit.B).longValue();
        }
        FAIL_ON_SIZE_BREACH = context.getProperty(FAILURE_ON_THRESHOLD_BREACH).asBoolean();
        FAIL_ON_SUB_ARCHIVE_FAILURE = context.getProperty(FAILURE_ON_SUB_ARCHIVE_UNPACKING).asBoolean();
        FAIL_ON_NOTHING_UNPACKED = context.getProperty(FAILURE_ON_NOTHING_UNPACKED).asBoolean();
        FAIL_ON_UNSUPPORTED = context.getProperty(FAILURE_ON_NON_UNSUPPORTED_ARCHIVE_FILES).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        InputStream inputStream = null;
        ArchiveInputStream arcInStream = null;
        final UnpackResult unpackResult = new UnpackResult();
        try {
            inputStream = new BufferedInputStream(session.read(flowFile));
            String mimeType = detectMimeType(inputStream);
            try {
                Exception exception = null;
                try {
                    arcInStream = ArchiveUtil.getArchiveInputStream(inputStream, mimeType, false);
                    unpack(session, flowFile, arcInStream, unpackResult);
                } catch (Exception e) {
                    exception = e; // we are only keeping this reference so that we can update the attributes for original FlowFile when transferred.
                    throw e;
                } finally {
                    IOUtils.closeQuietly(arcInStream, inputStream);
                    final Map<String, String> att = new HashMap<>();
                    att.put(UNPACK_SIZE_THRESHOLD_BREACHED, String.valueOf(unpackResult.thresholdReached()));
                    att.put(DDCoreAttributes.MIME_TYPE, mimeType);
                    att.put(UNPACKED_BYTES_ATTRIBUTE, String.valueOf(unpackResult.bytes()));
                    if (Objects.nonNull(exception)) {
                        att.put(DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, exception.getMessage());
                        att.put(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, exception.getClass().getName());
                    }
                    flowFile = session.putAllAttributes(flowFile, att);
                    if (!unpackResult.unpackedFiles().isEmpty()) {
                        final String fragmentId = unpackResult.unpackedFiles().get(0).getAttribute(DDCoreAttributes.DDFragmentAttributes.FRAGMENT_IDENTIFIER);
                        flowFile = FragmentAttributes.copyAttributesToOriginal(session, flowFile, fragmentId, unpackResult.unpackedFiles().size());
                    }
                }

                if (FAIL_ON_NOTHING_UNPACKED && unpackResult.unpackedFiles().isEmpty()) {
                    throw new NothingUnpackedException("Nothing unpacked from the archive");
                }
                if (!unpackResult.unpackedFiles().isEmpty()) {
                    prepareUnpackedFlowFiles(session, unpackResult.unpackedFiles());
                    session.getProvenanceReporter().fork(flowFile, unpackResult.unpackedFiles());
                    session.transfer(unpackResult.unpackedFiles(), REL_SUCCESS);
                    unpackResult.unpackedFiles().clear();
                }
                session.transfer(flowFile, REL_ORIGINAL);
            } catch (NothingUnpackedException e) {
                route(session, flowFile, e, REL_FAILURE);
                log.warn("Nothing unpacked from the archive", e);
            } catch (UnpackSizeThresholdReachedException e) {
                route(session, flowFile, e, REL_FAILURE);
                log.warn("Unpack Size Threshold Reached for the archive file", e);
            } catch (NotAnArchiveOrSupportedArchiveException e) {
                if (FAIL_ON_UNSUPPORTED) {
                    route(session, flowFile, e, REL_FAILURE);
                    log.warn("Not an archive file", e);
                } else {
                    route(session, flowFile, e, REL_ORIGINAL);
                }
            } catch (Exception e) {
                route(session, flowFile, e instanceof ProcessSession ? new Exception(e.getCause()) : e, REL_FAILURE);
                log.error("Error while unpacking the file", e);
            } finally {
                cleanQuietly(session, unpackResult.unpackedFiles());
            }
        } catch (Exception e) {
            IOUtils.closeQuietly(arcInStream, inputStream);
            route(session, flowFile, e instanceof ProcessSession ? new Exception(e.getCause()) : e, REL_FAILURE);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        String sizeLimit = validationContext.getProperty(UNPACK_SIZE_THRESHOLD).evaluateAttributeExpressions().getValue();
        if (sizeLimit != null && !"-1".equals(sizeLimit.trim())) {
            ValidationResult vr = StandardValidators.DATA_SIZE_VALIDATOR.validate(UNPACK_SIZE_THRESHOLD.getName(), sizeLimit, validationContext);
            if (!vr.isValid()) {
                results.add(vr);
            }
        }
        return results;
    }

    /**
     * This method detects the Mime-Type of the input stream, also it removes any additional information from the detected Mime-Type like version, charset etc.
     * 
     * @param inputStream for which Mime-Type should be identified.
     * @return identified Mime-Type of the input stream.
     * @throws java.io.IOException in case any IOException is raised.
     */
    private String detectMimeType(final InputStream inputStream) throws IOException {
        return detector.detect(inputStream, new Metadata()).toString().split(SEMICOLON)[0];
    }

    /**
     * @param session process session.
     * @param flowFile FlowFile.
     * @param archiveInputStream to unpack.
     */
    private void unpack(final ProcessSession session, final FlowFile flowFile, final ArchiveInputStream archiveInputStream, final UnpackResult upResult) throws IOException, CompressorException, ArchiveException, NotAnArchiveOrSupportedArchiveException {
        List<ArchiveInputStream> subArchives = new ArrayList<>();
        try {
            ArchiveEntry arcEntry;
            while ((arcEntry = archiveInputStream.getNextEntry()) != null) {
                if (!arcEntry.isDirectory()) {
                    long entrySize = arcEntry.getSize();
                    if (SIZE_THRESHOLD != -1L && (entrySize + upResult.bytes() > SIZE_THRESHOLD)) {
                        upResult.setThresholdReached(true);
                        if (FAIL_ON_SIZE_BREACH) {
                            throw new UnpackSizeThresholdReachedException("Unpack threshold reached, archive has more data then the specified limit.");
                        }
                    } else {
                        InputStream entryStream = null;
                        try {
                            entryStream = ArchiveUtil.getEntryInputStream(archiveInputStream);
                        } catch (Exception e) {
                            IOUtils.closeQuietly(entryStream);
                            throw e;
                        }
                        String entryMimeType = OCTET_STREAM;
                        if (RECURSE_UNPACK) {
                            entryMimeType = detectMimeType(entryStream);
                            ArchiveInputStream subarc = ArchiveUtil.getArchiveInputStream(entryStream, entryMimeType, true);
                            if (Objects.nonNull(subarc)) {
                                subArchives.add(subarc);
                                continue; // this is important
                            }
                        }
                        try {
                            upResult.unpackedFiles().add(createEntryFlowFile(session, flowFile, entryStream, entryMimeType, arcEntry));
                            upResult.addBytes(entrySize);
                        } finally {
                            IOUtils.closeQuietly(entryStream);
                        }
                    }
                }
            }
            // process sub archives
            for (ArchiveInputStream sais : subArchives) {
                try {
                    unpack(session, flowFile, sais, upResult);
                } catch (UnpackSizeThresholdReachedException ustre) {
                    throw ustre;
                } catch (Exception e) {
                    if (FAIL_ON_SUB_ARCHIVE_FAILURE) {
                        throw e;
                    }
                } finally {
                    IOUtils.closeQuietly(sais);
                }
            }
        } finally {
            ArchiveUtil.closeQuietly(subArchives);
            ArchiveUtil.closeQuietly(List.of(archiveInputStream));
            subArchives.clear();
        }
    }

    /**
     * This method creates the child flow file with the unpacked entry as content. Provided InputStream will be closed by this method.
     * 
     * @param session process session.
     * @param parentFlowFile parent FlowFile.
     * @param inputStream of the entry to read content from.
     * @param entryMimeType mime.type of the child FlowFile.
     * @param arcEntry archive entry to set the new attributes of the child FlowFile.
     * @return new created child FlowFile.
     */
    private FlowFile createEntryFlowFile(final ProcessSession session, final FlowFile parentFlowFile, final InputStream inputStream, final String entryMimeType, final ArchiveEntry arcEntry) {
        FlowFile nff = null;
        try {
            nff = session.create(parentFlowFile);
            nff = session.importFrom(inputStream, nff);
            nff = setAttributes(session, parentFlowFile, nff, entryMimeType, arcEntry);
        } catch (Exception e) {
            if (nff != null) {
                session.remove(nff);
            }
            throw e;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return nff;
    }

    /**
     * @param session process session to work with.
     * @param parentFlowFile parent FlowFile to calculate new attributes for child FlowFile.
     * @param childFlowFile child FlowFile for which attributes needs to be set.
     * @param entryMimeType child FlowFile mime.type.
     * @param archiveEntry archive entry to calculate new attributes for the child FlowFile.
     * @return same child FlowFile with new attributes.
     */
    private FlowFile setAttributes(final ProcessSession session, final FlowFile parentFlowFile, FlowFile childFlowFile, final String entryMimeType, final ArchiveEntry archiveEntry) {
        final Map<String, String> newAttributes = getNewAttributes(parentFlowFile, archiveEntry);
        newAttributes.put(DDCoreAttributes.MIME_TYPE, Objects.nonNull(entryMimeType) ? entryMimeType : OCTET_STREAM);
        return session.putAllAttributes(childFlowFile, newAttributes);
    }

    /**
     * This method provides the attributes map which needs to be set for child FlowFiles. *
     * 
     * @param parentFlowFile used to get the filename of the parent FlowFile.
     * @param arcEntry to get the details from and put it into the map.
     * @return attributes for the child FlowFile.
     */
    private Map<String, String> getNewAttributes(final FlowFile parentFlowFile, final ArchiveEntry arcEntry) {
        final Map<String, String> newAttributes = new HashMap<>();
        newAttributes.put(DDCoreAttributes.DDFragmentAttributes.FRAGMENT_IDENTIFIER, UUID.randomUUID().toString());

        final Path entryPath = Paths.get(arcEntry.getName());
        final Path parentPath = entryPath.getParent();
        String path = Objects.isNull(parentPath) ? "/" : parentPath + "/";

        final String parentFileName = parentFlowFile.getAttribute(DDCoreAttributes.FILENAME);
        String absolutePath = parentFileName + "/" + (Objects.nonNull(parentPath) ? parentPath : "");

        newAttributes.put(DDCoreAttributes.FILENAME, entryPath.getFileName().toString());
        newAttributes.put(DDCoreAttributes.PATH, path);
        newAttributes.put(DDCoreAttributes.ABSOLUTE_PATH, absolutePath);
        newAttributes.put(DDCoreAttributes.MIME_TYPE, OCTET_STREAM);
        newAttributes.put(DDCoreAttributes.DDFragmentAttributes.SEGMENT_ORIGINAL_FILENAME, parentFileName);
        newAttributes.put(DDCoreAttributes.DDFragmentAttributes.SEGMENT_PATH, path);

        if (arcEntry instanceof TarArchiveEntry) {
            TarArchiveEntry tae = (TarArchiveEntry) arcEntry;
            newAttributes.put(FILE_PERMISSIONS_ATTRIBUTE, permissionToString(tae.getMode()));
            newAttributes.put(FILE_OWNER_ATTRIBUTE, String.valueOf(tae.getUserName()));
            newAttributes.put(FILE_GROUP_ATTRIBUTE, String.valueOf(tae.getGroupName()));
            final String timeAsString = DATE_TIME_FORMATTER.format(tae.getModTime().toInstant());
            newAttributes.put(FILE_LAST_MODIFIED_TIME_ATTRIBUTE, timeAsString);
            newAttributes.put(FILE_CREATION_TIME_ATTRIBUTE, timeAsString);
        }
        return newAttributes;
    }

    /**
     * Converts octal int value to human readable permissions
     * 
     * @param octalValue integer octal value
     * @return human readable string representation of the octal permission
     */
    public static String permissionToString(int octalValue) {
        if (octalValue < 0 || octalValue > 777) {
            throw new IllegalArgumentException("Invalid octal value");
        }

        String rwx = "rwx";
        StringBuilder permission = new StringBuilder("---------");
        for (int i = 0; i < 3; i++) {
            int digit = octalValue % 10;
            octalValue /= 10;
            for (int j = 0; j < 3; j++) {
                if ((digit & (1 << j)) != 0) {
                    permission.setCharAt(8 - (i * 3 + j), rwx.charAt(2 - j));
                }
            }
        }
        return permission.toString();
    }

    /**
     * This method sets the fragment counts and fragment index for each FlowFile.
     * 
     * @param session process session.
     * @param flowFiles for which count and index attribute should be put.
     */
    private void prepareUnpackedFlowFiles(ProcessSession session, List<FlowFile> flowFiles) {
        ArrayList<FlowFile> newFlowFiles = new ArrayList<>(flowFiles);
        flowFiles.clear();
        for (int i = 0; i < newFlowFiles.size(); i++) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(DDCoreAttributes.DDFragmentAttributes.FRAGMENT_COUNT, String.valueOf(newFlowFiles.size()));
            attributes.put(DDCoreAttributes.DDFragmentAttributes.FRAGMENT_INDEX, String.valueOf(i + 1));
            FlowFile newFF = session.putAllAttributes(newFlowFiles.get(i), attributes);
            flowFiles.add(newFF);
        }
        newFlowFiles.clear();
    }

    /**
     * This method will transfer the FlowFile to given relation.
     *
     * @param session process session.
     * @param flowFile FlowFile.
     * @param exception exception for which exception attributes should be added on the outgoing FlowFile.
     * @see io.github.deepakdaneva.nifi.core.DDCoreAttributes for exception related attributes.
     */
    private void route(final ProcessSession session, FlowFile flowFile, final Exception exception, final Relationship relation) {
        if (Objects.nonNull(exception)) {
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, exception.getMessage());
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, exception.getClass().getName());
            session.getProvenanceReporter().modifyAttributes(flowFile);
        }
        session.transfer(flowFile, relation);
    }

    /**
     * @param session process session to remove stale flow files.
     * @param staleFlowFiles list of stale flow files which needs to be removed.
     */
    private void cleanQuietly(final ProcessSession session, final List<FlowFile> staleFlowFiles) {
        if (Objects.nonNull(session)) {
            if (Objects.nonNull(staleFlowFiles) && !staleFlowFiles.isEmpty()) {
                staleFlowFiles.forEach(ff -> {
                    try {
                        session.remove(ff);
                    } catch (Exception ignored) {
                    }
                });
                staleFlowFiles.clear();
            }
        }
    }
}

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
class UnpackResult {
    private final List<FlowFile> unpackedFiles;
    private final AtomicLong bytes;
    private final AtomicBoolean thresholdReached;

    public UnpackResult() {
        unpackedFiles = new ArrayList<>();
        bytes = new AtomicLong(0);
        thresholdReached = new AtomicBoolean(false);
    }

    public List<FlowFile> unpackedFiles() {
        return unpackedFiles;
    }

    public void addBytes(long bytesExtracted) {
        this.bytes.addAndGet(bytesExtracted);
    }

    public long bytes() {
        return bytes.get();
    }

    public void setThresholdReached(boolean thresholdReached) {
        if (thresholdReached) {
            this.thresholdReached.compareAndSet(false, true);
        }
    }

    public boolean thresholdReached() {
        return thresholdReached.get();
    }

}
