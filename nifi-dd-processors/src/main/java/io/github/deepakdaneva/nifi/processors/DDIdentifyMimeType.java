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

import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
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
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detects the MIME type of a FlowFile with Apache Tika.
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, DDCoreAttributes.MIME_TYPE, DDIdentifyMimeType.DD_MIME_EXTENSION, "compression", "gzip", "bzip2", "zip", "MIME", "file", "identify", "tika"})
@CapabilityDescription("Attempts to identify the MIME type of a FlowFile from its content using Apache Tika. The '" + DDCoreAttributes.MIME_TYPE + "' attribute is set to the detected MIME type, or 'application/octet-stream' if it cannot be determined, and '" + DDIdentifyMimeType.DD_MIME_EXTENSION + "' is set to a common file extension for the MIME type when one is known. If neither a MIME config file nor body is set, Tika's default MIME types are used; " + "otherwise only the MIME types of the custom configuration (Tika mimetypes XML format) are used.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.MIME_TYPE, description = "Detected MIME type."), @WritesAttribute(attribute = DDIdentifyMimeType.DD_MIME_EXTENSION, description = "Common file extension of the detected MIME type including the leading dot, or empty if unknown.")})
@SeeAlso({DDExtractText.class, DDArchiveContainsFileType.class})
public class DDIdentifyMimeType extends AbstractDDProcessor {

    /**
     * Attribute holding the file extension of the detected MIME type.
     */
    public static final String DD_MIME_EXTENSION = "mime.extension";

    /**
     * Removes parameters from the detected MIME type.
     */
    public static final PropertyDescriptor CLEAN_MIME_TYPE_ONLY = new PropertyDescriptor.Builder().name("Clean Mime Type Only").description("Detected MIME types may carry parameters (e.g. 'application/x-rar-compressed; version=5'). If true, parameters are removed (e.g. 'application/x-rar-compressed').").required(true).allowableValues("true", "false").defaultValue("true").build();

    /**
     * Custom MIME types file.
     */
    public static final PropertyDescriptor MIME_CONFIG_FILE = new PropertyDescriptor.Builder().name("Mime Config File").description("Path to a custom MIME types file (Tika mimetypes XML format). Only one of config file or config body may be set.").required(false).identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).build();

    /**
     * Custom MIME types content.
     */
    public static final PropertyDescriptor MIME_CONFIG_BODY = new PropertyDescriptor.Builder().name("Mime Config Body").description("Content of a custom MIME types file (Tika mimetypes XML format). Only one of config file or config body may be set.").required(false).addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.NONE).build();

    /**
     * Relationship for all FlowFiles.
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles are routed to success.").build();

    private static final String GZIP = "application/gzip";
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(CLEAN_MIME_TYPE_ONLY, MIME_CONFIG_FILE, MIME_CONFIG_BODY);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile Detector detector;
    private volatile MimeTypes mimeTypes;
    private volatile boolean cleanMimeType;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Loads the MIME type configuration when the processor is scheduled.
     *
     * @param context process context.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        cleanMimeType = context.getProperty(CLEAN_MIME_TYPE_ONLY).asBoolean();
        final String configBody = context.getProperty(MIME_CONFIG_BODY).getValue();
        try {
            if (configBody != null) {
                mimeTypes = MimeTypesFactory.create(new ByteArrayInputStream(configBody.getBytes(StandardCharsets.UTF_8)));
                detector = mimeTypes;
            } else if (context.getProperty(MIME_CONFIG_FILE).isSet()) {
                try (InputStream in = context.getProperty(MIME_CONFIG_FILE).evaluateAttributeExpressions().asResource().read()) {
                    mimeTypes = MimeTypesFactory.create(in);
                    detector = mimeTypes;
                }
            } else {
                final TikaConfig config = TikaConfig.getDefaultConfig();
                detector = config.getDetector();
                mimeTypes = config.getMimeRepository();
            }
        } catch (final IOException | MimeTypeException e) {
            throw new ProcessException("Failed to load MIME type configuration", e);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(MIME_CONFIG_BODY).isSet() && validationContext.getProperty(MIME_CONFIG_FILE).isSet()) {
            results.add(new ValidationResult.Builder().subject(MIME_CONFIG_FILE.getName()).valid(false).explanation("only one of '" + MIME_CONFIG_BODY.getName() + "' or '" + MIME_CONFIG_FILE.getName() + "' can be set").build());
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String[] detected = new String[1];
        session.read(flowFile, in -> {
            try (TikaInputStream tikaStream = TikaInputStream.get(in)) {
                detected[0] = detector.detect(tikaStream, new Metadata()).toString();
            }
        });

        final String mimeType = detected[0] == null ? OCTET_STREAM : detected[0];
        String extension = "";
        try {
            extension = mimeTypes.forName(stripMimeParameters(mimeType)).getExtension();
        } catch (final MimeTypeException e) {
            getLogger().warn("MIME type extension lookup failed for {}", mimeType, e);
        }
        // Tika reports .tgz as the first extension of application/gzip (TIKA-1563)
        if (GZIP.equals(stripMimeParameters(mimeType)) && ".tgz".equals(extension)) {
            extension = ".gz";
        }

        flowFile = session.putAllAttributes(flowFile, Map.of(DDCoreAttributes.MIME_TYPE, cleanMimeType ? stripMimeParameters(mimeType) : mimeType, DD_MIME_EXTENSION, extension));
        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }
}
