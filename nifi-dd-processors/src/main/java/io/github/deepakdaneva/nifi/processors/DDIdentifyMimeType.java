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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, DDCoreAttributes.MIME_TYPE, DDIdentifyMimeType.DD_MIME_EXTENSION, "compression", "gzip", "bzip2", "zip", "MIME", "file", "identify"})
@CapabilityDescription("Attempts to identify the MIME Type of a given FlowFile. If the MIME Type can be identified, an attribute with the name '" + DDCoreAttributes.MIME_TYPE + "' is added with the value being the MIME Type. If the MIME Type cannot be determined, the value will be set to 'application/octet-stream'. In addition, the attribute '" + DDIdentifyMimeType.DD_MIME_EXTENSION + "' will be set if a common file extension for the MIME Type is known. If both Config File and Config Body are not set, the default NiFi MIME Types will be used. NOTE: This processor is backed by Apache Tika visit https://tika.apache.org to know more.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.MIME_TYPE, description = "Detected MIME Type will be provided by this attribute."), @WritesAttribute(attribute = DDIdentifyMimeType.DD_MIME_EXTENSION, description = "Detected mime extension will be provided by this attribute."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
public class DDIdentifyMimeType extends AbstractProcessor {

    public static final PropertyDescriptor CLEAN_MIME_TYPE_ONLY = new PropertyDescriptor.Builder().name("Clean Mime Type Only").description("Sometimes when mime type is detected then other information in the mime type string is also included (e.g. 'application/x-rar-compressed; version=5'). If true then final mime type string will not include any other information (e.g. 'application/x-rar-compressed').").required(true).allowableValues("true", "false").defaultValue("true").build();

    public static final PropertyDescriptor MIME_CONFIG_FILE = new PropertyDescriptor.Builder().name("Mime Config File").required(false).description("Path to MIME type config file. Only one of Config File or Config Body may be used.").addValidator(StandardValidators.NON_BLANK_VALIDATOR).addValidator(new StandardValidators.FileExistsValidator(true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

    public static final PropertyDescriptor MIME_CONFIG_BODY = new PropertyDescriptor.Builder().name("Mime Config Body").required(false).description("Body of MIME type config file. Only one of Config File or Config Body may be used.").addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.NONE).build();
    public static final String DD_MIME_EXTENSION = "mime.extension";
    public static final String SEMICOLON = ";";
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles are routed to success").build();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private Detector detector;
    private MimeTypes mimeTypes;
    private boolean CLEAN_MIME_TYPE;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        properties = List.of(CLEAN_MIME_TYPE_ONLY, MIME_CONFIG_FILE, MIME_CONFIG_BODY);
        relationships = Set.of(REL_SUCCESS);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String configBody = context.getProperty(MIME_CONFIG_BODY).getValue();
        String configFile = context.getProperty(MIME_CONFIG_FILE).evaluateAttributeExpressions().getValue();
        CLEAN_MIME_TYPE = context.getProperty(CLEAN_MIME_TYPE_ONLY).asBoolean();
        if (configBody == null && configFile == null) {
            TikaConfig tikaDefaultConfig = TikaConfig.getDefaultConfig();
            detector = tikaDefaultConfig.getDetector();
            mimeTypes = tikaDefaultConfig.getMimeRepository();
        } else if (configBody != null) {
            try {
                detector = MimeTypesFactory.create(new ByteArrayInputStream(configBody.getBytes()));
                mimeTypes = (MimeTypes) detector;
            } catch (Exception e) {
                context.yield();
                throw new ProcessException("Failed to load mime config body", e);
            }
        } else {
            try (final FileInputStream fis = new FileInputStream(configFile); final InputStream bis = new BufferedInputStream(fis)) {
                detector = MimeTypesFactory.create(bis);
                mimeTypes = (MimeTypes) detector;
            } catch (Exception e) {
                context.yield();
                throw new ProcessException("Failed to load mime config file", e);
            }
        }
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog log = getLogger();
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);

        session.read(flowFile, stream -> {
            try (final InputStream in = new BufferedInputStream(stream); final TikaInputStream tikaStream = TikaInputStream.get(in)) {
                MediaType mediatype = detector.detect(tikaStream, new Metadata());
                mimeTypeRef.set(mediatype.toString());
            }
        });

        String mimeType = mimeTypeRef.get();
        String extension = "";
        try {
            MimeType mimetype;
            mimetype = mimeTypes.forName(mimeType);
            extension = mimetype.getExtension();
        } catch (MimeTypeException ex) {
            log.warn("MIME type extension lookup failed: {}", ex);
        }

        // Workaround for bug in Tika - https://issues.apache.org/jira/browse/TIKA-1563
        if (mimeType != null && mimeType.equals("application/gzip") && extension.equals(".tgz")) {
            extension = ".gz";
        }

        if (mimeType == null) {
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.MIME_TYPE, "application/octet-stream");
            flowFile = session.putAttribute(flowFile, DD_MIME_EXTENSION, "");
            log.info("Unable to identify MIME Type for {}; setting to application/octet-stream", flowFile);
        } else {
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.MIME_TYPE, CLEAN_MIME_TYPE ? mimeType.split(SEMICOLON)[0] : mimeType);
            flowFile = session.putAttribute(flowFile, DD_MIME_EXTENSION, extension);
        }

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        String body = validationContext.getProperty(MIME_CONFIG_BODY).getValue();
        String file = validationContext.getProperty(MIME_CONFIG_FILE).evaluateAttributeExpressions().getValue();
        if (body != null && file != null) {
            results.add(new ValidationResult.Builder().input(MIME_CONFIG_FILE.getName()).subject(file).valid(false).explanation("can only specify '" + MIME_CONFIG_BODY.getName() + "' or '" + MIME_CONFIG_FILE.getName() + "'. Not both.").build());
        }
        return results;
    }
}