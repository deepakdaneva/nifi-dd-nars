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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "mime", "content", "content-type", "identify", "zip", "archive"})
@CapabilityDescription("Attempts to search for any subfile mime type within the archive that matches one of the specified mime types given by the json array body or json array file path. This processor will stop processing the file further as soon as it found a match and will transfer the flowfile to success. NOTE: 1) Any exceptions raised during the detection of subfile mime type will be ignored. NOTE: This processor is backed by Apache Tika (https://tika.apache.org) and Apache Commons Compress (https://commons.apache.org/proper/commons-compress) check these to know more.")
@WritesAttributes({@WritesAttribute(attribute = DDArchiveContainsFileType.DD_ARCHIVE_MIME_TYPE, description = "Detected mime type of the given archive file will be provided by this attribute."), @WritesAttribute(attribute = DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, description = "Boolean flag whether any match if found or not."), @WritesAttribute(attribute = DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, description = "String mime type for which match is found."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
public class DDArchiveContainsFileType extends AbstractProcessor {

    public static final PropertyDescriptor ROUTE_TO_FAILURE = new PropertyDescriptor.Builder().name("Non Archive/Unsupported to Failure").description("If true will route all non archive files or unsupported files to failure.").required(true).allowableValues("true", "false").defaultValue("true").build();
    public static final PropertyDescriptor RECURSE_SUB_ARCHIVES = new PropertyDescriptor.Builder().name("Recurse Sub Archives").description("If true will checking all the supported sub archive files recursively.").required(true).allowableValues("true", "false").defaultValue("false").build();
    public static final PropertyDescriptor FAIL_ON_SUB_ARCHIVES = new PropertyDescriptor.Builder().name("Fail on Sub Archives").description("If true and any exception is raised during sub archive processing then will route the FlowFile to failure or else continue processing other entries of the archive ignoring the sub archive exceptions.").required(true).allowableValues("true", "false").defaultValue("false").build();
    public static final PropertyDescriptor MIME_TYPES_CONFIG_FILE = new PropertyDescriptor.Builder().name("Search Mime Types Config File").required(false).description("Path to Json Array MIME type config file having mime types of subfile to search for. Only one of Config File or Config Body may be used.").addValidator(StandardValidators.NON_BLANK_VALIDATOR).addValidator(new StandardValidators.FileExistsValidator(true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
    public static final PropertyDescriptor MIME_TYPES_CONFIG_BODY = new PropertyDescriptor.Builder().name("Search Mime Types Config Body").required(false).description("Json Array Body of MIME type config file having mime types of subfile to search for. Only one of Config File or Config Body may be used.").addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.NONE).build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles are routed to success").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("All FlowFiles are routed to failure").build();
    static final String DD_ARCHIVE_MIME_TYPE = "dd.archive.mime.type";
    static final String DD_ARCHIVE_SUBFILE_MATCH_FOUND = "dd.archive.subfile.match.found";
    static final String DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE = "dd.archive.subfile.match.found.type";
    public static final String SEMICOLON = ";";
    private final ObjectMapper objectMapper;
    private Set<String> MIME_TYPES_TO_SEARCH;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private boolean ROUTE_TO_FAILURE_ON_ERROR;
    private boolean CHECK_SUB_ARCHIVES;
    private boolean FAIL_ON_SUB_ARCHIVE_FAILURES;
    private Detector detector;
    private ComponentLog log;

    public DDArchiveContainsFileType() {
        objectMapper = new ObjectMapper();
        MIME_TYPES_TO_SEARCH = new HashSet<>();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        properties = List.of(ROUTE_TO_FAILURE, RECURSE_SUB_ARCHIVES, FAIL_ON_SUB_ARCHIVES, MIME_TYPES_CONFIG_FILE, MIME_TYPES_CONFIG_BODY);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
        log = getLogger();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        initializeMimesToSearch(context);
        ROUTE_TO_FAILURE_ON_ERROR = context.getProperty(ROUTE_TO_FAILURE).asBoolean();
        CHECK_SUB_ARCHIVES = context.getProperty(RECURSE_SUB_ARCHIVES).asBoolean();
        FAIL_ON_SUB_ARCHIVE_FAILURES = context.getProperty(FAIL_ON_SUB_ARCHIVES).asBoolean();
        detector = new DefaultDetector();
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        MIME_TYPES_TO_SEARCH.clear();
    }

    private void initializeMimesToSearch(final ProcessContext context) {
        String body = context.getProperty(MIME_TYPES_CONFIG_BODY).getValue();
        String file = context.getProperty(MIME_TYPES_CONFIG_FILE).getValue();
        if (body != null) {
            try {
                MIME_TYPES_TO_SEARCH = objectMapper.readValue(body, new TypeReference<>() {
                });
            } catch (Exception e) {
                context.yield();
                throw new ProcessException("Failed to load mime types config body, it's not a valid json or not a valid string array json (i.e. [\"application/pdf\",\"application/zip\"])", e);
            }
        } else if (file != null) {
            try {
                MIME_TYPES_TO_SEARCH = objectMapper.readValue(Paths.get(file).toFile(), new TypeReference<>() {
                });
            } catch (Exception e) {
                context.yield();
                throw new ProcessException("Failed to load mime types config file, it's not a valid json file or not a valid string array json (i.e. [\"application/pdf\",\"application/zip\"])", e);
            }
        }
        MIME_TYPES_TO_SEARCH.removeIf(e -> e == null || e.trim().isEmpty());
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        InputStream inputStream = null;
        ArchiveInputStream archiveInputStream = null;
        try {
            inputStream = new BufferedInputStream(session.read(flowFile));
            String mimeType = detector.detect(inputStream, new Metadata()).toString().split(SEMICOLON)[0];
            archiveInputStream = ArchiveUtil.getArchiveInputStream(inputStream, mimeType, false);
            if (Objects.nonNull(archiveInputStream) && Objects.nonNull(MIME_TYPES_TO_SEARCH) && !MIME_TYPES_TO_SEARCH.isEmpty()) {
                String foundMimeType = findMimeType(archiveInputStream);
                IOUtils.closeQuietly(archiveInputStream, inputStream);
                routeToSuccess(session, flowFile, mimeType, foundMimeType);
            } else {
                IOUtils.closeQuietly(archiveInputStream, inputStream);
                routeToSuccess(session, flowFile, mimeType, null);
            }
        } catch (NotAnArchiveOrSupportedArchiveException e) {
            IOUtils.closeQuietly(archiveInputStream, inputStream);
            if (ROUTE_TO_FAILURE_ON_ERROR) {
                route(session, flowFile, e, REL_FAILURE);
            } else {
                route(session, flowFile, e, REL_SUCCESS);
            }
        } catch (Exception e) {
            IOUtils.closeQuietly(archiveInputStream, inputStream);
            route(session, flowFile, e, REL_FAILURE);
        }
    }

    /**
     * This method find the mime type of the entries inside the archive (or sub archives). This method will not close the provided ArchiveInputStream (but will close all sub-archives).
     * 
     * @param ais ArchiveInputStream instance of the archive file.
     * @return found mime-type inside the archive from the provided mime-type list.
     */
    private String findMimeType(final ArchiveInputStream ais) throws CompressorException, IOException, ArchiveException, NotAnArchiveOrSupportedArchiveException {
        List<ArchiveInputStream> subArchives = new ArrayList<>();
        try {
            ArchiveEntry arcEntry;
            while ((arcEntry = ais.getNextEntry()) != null) {
                if (!arcEntry.isDirectory()) {
                    InputStream entryInputStream = null;
                    boolean closeStream = true;
                    try {
                        entryInputStream = ArchiveUtil.getEntryInputStream(ais);
                        String entryType = detectMimeType(entryInputStream);
                        if (CHECK_SUB_ARCHIVES) {
                            ArchiveInputStream subArcStream = ArchiveUtil.getArchiveInputStream(entryInputStream, entryType, true);
                            if (Objects.nonNull(subArcStream)) {
                                subArchives.add(subArcStream);
                                closeStream = false;
                                continue;
                            }
                        }
                        if (MIME_TYPES_TO_SEARCH.contains(entryType)) {
                            return entryType;
                        }
                    } finally {
                        if (closeStream) {
                            IOUtils.closeQuietly(entryInputStream);
                        }
                    }
                }
            }
            // check in sub archive
            for (ArchiveInputStream sais : subArchives) {
                try {
                    String sMimeType = findMimeType(sais);
                    if (MIME_TYPES_TO_SEARCH.contains(sMimeType)) {
                        return sMimeType;
                    }
                } catch (Exception e) {
                    if (FAIL_ON_SUB_ARCHIVE_FAILURES) {
                        throw e;
                    } else {
                        log.warn("Error while processing sub-archive", e);
                    }
                } finally {
                    IOUtils.closeQuietly(sais);
                }
            }
        } finally {
            ArchiveUtil.closeQuietly(subArchives);
        }
        return null;
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
     * This method routes the FlowFile to the given relationship with the provided exception if any, if provided exception is null then it'll not set the exception attributes.
     *
     * @param session process session.
     * @param flowFile flowFile.
     * @param exception instance.
     */
    private void route(final ProcessSession session, FlowFile flowFile, final Exception exception, final Relationship relationship) {
        if (Objects.nonNull(exception)) {
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, exception.getMessage());
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, exception.getClass().getName());
        }
        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, relationship);
        if (relationship.equals(REL_FAILURE)) {
            log.error("exception raised while searching the supported mime types inside archive", exception);
        }
    }

    /**
     * This method will route the FlowFile to success.
     * 
     * @param session process session.
     * @param flowFile FlowFile.
     * @param archiveMimeType processed archive detected mime type.
     * @param foundMimeType found mime type inside the archive.
     */
    private void routeToSuccess(ProcessSession session, FlowFile flowFile, String archiveMimeType, String foundMimeType) {
        flowFile = session.putAttribute(flowFile, DD_ARCHIVE_SUBFILE_MATCH_FOUND, String.valueOf(foundMimeType != null));
        if (archiveMimeType != null) {
            flowFile = session.putAttribute(flowFile, DD_ARCHIVE_MIME_TYPE, archiveMimeType);
        }
        if (foundMimeType != null) {
            flowFile = session.putAttribute(flowFile, DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, foundMimeType);
        }
        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * This method validates the processor when scheduled.
     * 
     * @param validationContext provides a mechanism for obtaining externally
     * managed values, such as property values and supplies convenience methods
     * for operating on those values
     *
     * @return ValidationResult of the executed validation.
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        String body = validationContext.getProperty(MIME_TYPES_CONFIG_BODY).getValue();
        String file = validationContext.getProperty(MIME_TYPES_CONFIG_FILE).evaluateAttributeExpressions().getValue();
        if (body != null && file != null) {
            results.add(new ValidationResult.Builder().input(MIME_TYPES_CONFIG_BODY.getName()).valid(false).explanation("only one property can be set, specify '" + MIME_TYPES_CONFIG_BODY.getName() + "' or '" + MIME_TYPES_CONFIG_FILE.getName() + "'. Not both.").build());
        } else if (Objects.isNull(body) && Objects.isNull(file)) {
            results.add(new ValidationResult.Builder().input(MIME_TYPES_CONFIG_BODY.getName()).valid(false).explanation("one of '" + MIME_TYPES_CONFIG_BODY.getName() + "' or '" + MIME_TYPES_CONFIG_FILE.getName() + "' is required. Both can not be empty.").build());
        }
        return results;
    }

}