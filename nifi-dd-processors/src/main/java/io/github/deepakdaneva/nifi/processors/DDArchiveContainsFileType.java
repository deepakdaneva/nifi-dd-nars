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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.deepakdaneva.commons.archive.ArchiveUtil;
import io.github.deepakdaneva.commons.archive.NotAnArchiveOrSupportedArchiveException;
import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Searches the entries of an archive FlowFile for any of a configured set of MIME types.
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "mime", "content", "content-type", "identify", "zip", "tar", "7z", "archive", "search"})
@CapabilityDescription("Detects the MIME type of every file inside an archive FlowFile and checks whether any of them matches one of the configured MIME types. " + "The search stops at the first match and the FlowFile, with its content unchanged, is routed to 'success' with attributes describing the result. " + "Supported formats are those of Apache Commons Compress (zip, jar, tar, 7z, ar, arj, cpio, dump and tar compressed with gzip, bzip2, xz, lzma, brotli, zstd, ...). " + "MIME types are detected with Apache Tika. Entries are streamed; nested 7z archives are buffered in memory.")
@WritesAttributes({@WritesAttribute(attribute = DDArchiveContainsFileType.DD_ARCHIVE_MIME_TYPE, description = "Detected MIME type of the incoming FlowFile."), @WritesAttribute(attribute = DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND, description = "'true' if an entry with one of the configured MIME types was found, otherwise 'false'."), @WritesAttribute(attribute = DDArchiveContainsFileType.DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, description = "The MIME type that matched. Only set when a match is found."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Nested 7z archives need random access and are buffered in memory while being inspected.")
@SeeAlso({DDUnpackContent.class, DDIdentifyMimeType.class})
public class DDArchiveContainsFileType extends AbstractDDProcessor {

    /**
     * Attribute holding the detected MIME type of the incoming FlowFile.
     */
    public static final String DD_ARCHIVE_MIME_TYPE = "dd.archive.mime.type";
    /**
     * Attribute holding whether a matching entry was found.
     */
    public static final String DD_ARCHIVE_SUBFILE_MATCH_FOUND = "dd.archive.subfile.match.found";
    /**
     * Attribute holding the matched MIME type.
     */
    public static final String DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE = "dd.archive.subfile.match.found.type";

    /**
     * Routes non-archive or unsupported FlowFiles to failure instead of success.
     */
    public static final PropertyDescriptor ROUTE_TO_FAILURE = new PropertyDescriptor.Builder().name("Non Archive/Unsupported to Failure").description("If true, FlowFiles that are not archives or are unsupported archives are routed to 'failure'; otherwise they are routed to 'success' with '" + DD_ARCHIVE_SUBFILE_MATCH_FOUND + "' set to 'false'.").required(true).allowableValues("true", "false").defaultValue("true").build();

    /**
     * Searches inside nested archives as well.
     */
    public static final PropertyDescriptor RECURSE_SUB_ARCHIVES = new PropertyDescriptor.Builder().name("Recurse Sub Archives").description("If true, supported archives found inside the archive are searched recursively.").required(true).allowableValues("true", "false").defaultValue("false").build();

    /**
     * Fails the FlowFile if a nested archive cannot be read.
     */
    public static final PropertyDescriptor FAIL_ON_SUB_ARCHIVES = new PropertyDescriptor.Builder().name("Fail on Sub Archives").description("If true and a nested archive cannot be read, the FlowFile is routed to 'failure'. If false, the unreadable nested archive is skipped and the search continues.").required(true).allowableValues("true", "false").defaultValue("false").dependsOn(RECURSE_SUB_ARCHIVES, "true").build();

    /**
     * Maximum depth of nested archives that are opened.
     */
    public static final PropertyDescriptor MAX_SUB_ARCHIVE_DEPTH = new PropertyDescriptor.Builder().name("Maximum Sub Archive Depth").description("Maximum nesting depth of archives that are opened when searching recursively. Archives nested deeper are matched by their own MIME type only. Protects against recursive archive bombs.").required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("10").dependsOn(RECURSE_SUB_ARCHIVES, "true").build();

    /**
     * Path of a JSON file listing the MIME types to search for.
     */
    public static final PropertyDescriptor MIME_TYPES_CONFIG_FILE = new PropertyDescriptor.Builder().name("Search Mime Types Config File").description("Path to a file containing a JSON array of MIME types to search for, e.g. [\"application/pdf\",\"text/plain\"]. Exactly one of the config file or config body must be set.").required(false).identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).build();

    /**
     * JSON array of the MIME types to search for.
     */
    public static final PropertyDescriptor MIME_TYPES_CONFIG_BODY = new PropertyDescriptor.Builder().name("Search Mime Types Config Body").description("JSON array of MIME types to search for, e.g. [\"application/pdf\",\"text/plain\"]. Exactly one of the config file or config body must be set.").required(false).addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.NONE).build();

    /**
     * Relationship for processed FlowFiles.
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles that were searched successfully, whether or not a match was found.").build();
    /**
     * Relationship for FlowFiles that could not be searched.
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that could not be searched.").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(ROUTE_TO_FAILURE, RECURSE_SUB_ARCHIVES, FAIL_ON_SUB_ARCHIVES, MAX_SUB_ARCHIVE_DEPTH, MIME_TYPES_CONFIG_FILE, MIME_TYPES_CONFIG_BODY);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {
    };

    private volatile Set<String> mimeTypesToSearch = Set.of();
    private volatile boolean routeUnsupportedToFailure;
    private volatile boolean recurse;
    private volatile boolean failOnSubArchiveError;
    private volatile int maxDepth;
    private volatile Detector detector;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Loads the configuration when the processor is scheduled.
     *
     * @param context process context.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        mimeTypesToSearch = loadMimeTypes(context);
        routeUnsupportedToFailure = context.getProperty(ROUTE_TO_FAILURE).asBoolean();
        recurse = context.getProperty(RECURSE_SUB_ARCHIVES).asBoolean();
        // dependent properties have no value when recursion is disabled
        failOnSubArchiveError = recurse && context.getProperty(FAIL_ON_SUB_ARCHIVES).asBoolean();
        maxDepth = recurse ? context.getProperty(MAX_SUB_ARCHIVE_DEPTH).asInteger() : 1;
        detector = TikaConfig.getDefaultConfig().getDetector();
    }

    private Set<String> loadMimeTypes(final ProcessContext context) {
        final String body = context.getProperty(MIME_TYPES_CONFIG_BODY).getValue();
        try {
            final List<String> types = body != null ? OBJECT_MAPPER.readValue(body, STRING_LIST) : OBJECT_MAPPER.readValue(context.getProperty(MIME_TYPES_CONFIG_FILE).evaluateAttributeExpressions().asResource().read(), STRING_LIST);
            return types.stream().filter(Objects::nonNull).map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toUnmodifiableSet());
        } catch (final IOException e) {
            throw new ProcessException("Failed to load MIME types; expected a JSON array of strings such as [\"application/pdf\",\"application/zip\"]", e);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final String body = validationContext.getProperty(MIME_TYPES_CONFIG_BODY).getValue();
        final boolean fileSet = validationContext.getProperty(MIME_TYPES_CONFIG_FILE).isSet();
        if (body != null && fileSet) {
            results.add(new ValidationResult.Builder().subject(MIME_TYPES_CONFIG_BODY.getName()).valid(false).explanation("only one of '" + MIME_TYPES_CONFIG_BODY.getName() + "' or '" + MIME_TYPES_CONFIG_FILE.getName() + "' can be set").build());
        } else if (body == null && !fileSet) {
            results.add(new ValidationResult.Builder().subject(MIME_TYPES_CONFIG_BODY.getName()).valid(false).explanation("one of '" + MIME_TYPES_CONFIG_BODY.getName() + "' or '" + MIME_TYPES_CONFIG_FILE.getName() + "' is required").build());
        } else if (body != null) {
            try {
                OBJECT_MAPPER.readValue(body, STRING_LIST);
            } catch (final IOException e) {
                results.add(new ValidationResult.Builder().subject(MIME_TYPES_CONFIG_BODY.getName()).input(body).valid(false).explanation("not a JSON array of strings: " + e.getMessage()).build());
            }
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String[] archiveMimeType = new String[1];
        final String found;
        try {
            found = searchFlowFile(session, flowFile, archiveMimeType);
        } catch (final NotAnArchiveOrSupportedArchiveException e) {
            getLogger().debug("{} is not a supported archive", flowFile, e);
            transferResult(session, flowFile, archiveMimeType[0], null, routeUnsupportedToFailure ? REL_FAILURE : REL_SUCCESS, e);
            return;
        } catch (final Exception e) {
            getLogger().error("Failed to search archive {}", flowFile, e);
            transferResult(session, flowFile, archiveMimeType[0], null, REL_FAILURE, e);
            return;
        }
        transferResult(session, flowFile, archiveMimeType[0], found, REL_SUCCESS, null);
    }

    /**
     * Reads the FlowFile content and searches it. The content stream is closed before returning so the FlowFile can be
     * modified and transferred afterwards.
     */
    private String searchFlowFile(final ProcessSession session, final FlowFile flowFile, final String[] archiveMimeType) throws Exception {
        try (InputStream in = TikaInputStream.get(new BufferedInputStream(session.read(flowFile)))) {
            archiveMimeType[0] = detectMimeType(detector, in);
            try (ArchiveInputStream<?> archive = ArchiveUtil.getArchiveInputStream(in, archiveMimeType[0])) {
                return mimeTypesToSearch.isEmpty() ? null : search(archive, 1);
            }
        }
    }

    /**
     * Depth-first search of an archive.
     *
     * @return the first matching MIME type or {@code null}.
     */
    private String search(final ArchiveInputStream<?> archive, final int depth) throws IOException {
        final String[] found = new String[1];
        ArchiveWalker.walk(archive, (entry, content) -> {
            final String entryType = detectMimeType(detector, content);
            if (mimeTypesToSearch.contains(entryType)) {
                found[0] = entryType;
                return false;
            }
            if (recurse && depth < maxDepth) {
                found[0] = searchNested(content, entryType, depth);
            }
            return found[0] == null;
        });
        return found[0];
    }

    private String searchNested(final InputStream content, final String entryType, final int depth) throws IOException {
        try {
            final ArchiveWalker.Nested nested = ArchiveWalker.openNested(content, entryType, NO_LIMIT);
            if (nested.archive() == null) {
                return null;
            }
            try (ArchiveInputStream<?> archive = nested.archive()) {
                return search(archive, depth + 1);
            }
        } catch (final IOException | RuntimeException e) {
            if (failOnSubArchiveError) {
                throw e;
            }
            getLogger().warn("Skipping unreadable nested archive of type {}", entryType, e);
            return null;
        }
    }

    private void transferResult(final ProcessSession session, FlowFile flowFile, final String archiveMimeType, final String found, final Relationship relationship, final Exception exception) {
        final Map<String, String> attributes = new HashMap<>(exceptionAttributes(exception));
        if (archiveMimeType != null) {
            attributes.put(DD_ARCHIVE_MIME_TYPE, archiveMimeType);
        }
        if (relationship == REL_SUCCESS) {
            attributes.put(DD_ARCHIVE_SUBFILE_MATCH_FOUND, String.valueOf(found != null));
            if (found != null) {
                attributes.put(DD_ARCHIVE_SUBFILE_MATCH_FOUND_TYPE, found);
            }
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, relationship);
    }
}
