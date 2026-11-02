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
import io.github.deepakdaneva.nifi.exceptions.EncryptedOrPasswordProtectedFileException;
import io.github.deepakdaneva.nifi.exceptions.TextExtractionLimitReachedException;
import io.github.deepakdaneva.nifi.exceptions.UnsupportedFileType;
import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.UnsupportedFormatException;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Replaces the content of a FlowFile with the plain text extracted from it by Apache Tika.
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "text", "content", "extraction", "tika", "pdf", "office"})
@CapabilityDescription("Parses the FlowFile content with Apache Tika and replaces it with the extracted plain text (UTF-8, leading and trailing whitespace removed). " + "The amount of extracted text can be limited by characters and by size. Text is streamed into the FlowFile content, so the extracted text is never held in memory as a whole.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.MIME_TYPE, description = "Set to 'text/plain' when text is extracted successfully."), @WritesAttribute(attribute = DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, description = "Set to 'true', on success or failure, when the document holds more text than the characters or size limit allows. Not set otherwise."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
@SeeAlso({DDIdentifyMimeType.class})
public class DDExtractText extends AbstractDDProcessor {

    /**
     * Attribute set when the text limit was reached.
     */
    public static final String TEXT_EXTRACTION_LIMIT_REACHED = "text.extraction.limit.reached";

    /**
     * Maximum size of extracted text.
     */
    public static final PropertyDescriptor EXTRACT_SIZE_LIMIT = new PropertyDescriptor.Builder().name("Text extraction size limit").description("Maximum size (UTF-8 encoded) of the extracted text, e.g. '80 MB'. Set -1 for no size limit.").required(true).addValidator(DATA_SIZE_OR_UNLIMITED_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).defaultValue("80 MB").build();

    /**
     * Maximum number of extracted characters.
     */
    public static final PropertyDescriptor EXTRACT_CHARACTERS_LIMIT = new PropertyDescriptor.Builder().name("Text extraction characters limit").description("Maximum number of characters to extract. Set -1 for no characters limit.").required(true).addValidator(POSITIVE_INTEGER_OR_UNLIMITED_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).defaultValue("100000").build();

    /**
     * Routes to failure when a limit is reached.
     */
    public static final PropertyDescriptor FAIL_ON_LIMIT_REACHED = new PropertyDescriptor.Builder().name("Fail on limit reached").description("If true and the characters or size limit is reached, the FlowFile is routed to 'failure' with its original content. If false, it is routed to 'success' with the text " + "extracted up to the limit. '" + TEXT_EXTRACTION_LIMIT_REACHED + "' is set in both cases.").required(true).allowableValues("true", "false").defaultValue("true").build();

    /**
     * Tika configuration file.
     */
    public static final PropertyDescriptor TIKA_CONFIG_FILE = new PropertyDescriptor.Builder().name("Tika Config File").description("Path to a Tika configuration (tika-config.xml) file. Only one of config file or config body may be set; if neither is set the default Tika configuration is used.").required(false).identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).build();

    /**
     * Tika configuration content.
     */
    public static final PropertyDescriptor TIKA_CONFIG_BODY = new PropertyDescriptor.Builder().name("Tika Config Body").description("Content of a Tika configuration (tika-config.xml). Only one of config file or config body may be set; if neither is set the default Tika configuration is used.").required(false).addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.NONE).build();

    /**
     * Relationship for FlowFiles whose text was extracted.
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles whose content was replaced by the extracted text.").build();
    /**
     * Relationship for FlowFiles whose text could not be extracted.
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles whose text could not be extracted, with their original content.").build();

    private static final String TEXT_PLAIN = "text/plain";
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(EXTRACT_SIZE_LIMIT, EXTRACT_CHARACTERS_LIMIT, FAIL_ON_LIMIT_REACHED, TIKA_CONFIG_FILE, TIKA_CONFIG_BODY);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    private volatile long sizeLimit;
    private volatile long charactersLimit;
    private volatile boolean failOnLimitReached;
    private volatile Parser parser;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Loads the configuration and creates the Tika parser when the processor is scheduled.
     *
     * @param context process context.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        sizeLimit = parseDataSizeOrUnlimited(context.getProperty(EXTRACT_SIZE_LIMIT));
        charactersLimit = Long.parseLong(context.getProperty(EXTRACT_CHARACTERS_LIMIT).evaluateAttributeExpressions().getValue().trim());
        failOnLimitReached = context.getProperty(FAIL_ON_LIMIT_REACHED).asBoolean();

        final String configBody = context.getProperty(TIKA_CONFIG_BODY).getValue();
        try {
            if (configBody != null) {
                parser = new AutoDetectParser(new TikaConfig(new ByteArrayInputStream(configBody.getBytes(StandardCharsets.UTF_8))));
            } else if (context.getProperty(TIKA_CONFIG_FILE).isSet()) {
                try (InputStream in = context.getProperty(TIKA_CONFIG_FILE).evaluateAttributeExpressions().asResource().read()) {
                    parser = new AutoDetectParser(new TikaConfig(in));
                }
            } else {
                parser = new AutoDetectParser(TikaConfig.getDefaultConfig());
            }
        } catch (final TikaException | IOException | SAXException e) {
            throw new ProcessException("Failed to load Tika configuration", e);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(TIKA_CONFIG_BODY).isSet() && validationContext.getProperty(TIKA_CONFIG_FILE).isSet()) {
            results.add(new ValidationResult.Builder().subject(TIKA_CONFIG_FILE.getName()).valid(false).explanation("only one of '" + TIKA_CONFIG_BODY.getName() + "' or '" + TIKA_CONFIG_FILE.getName() + "' can be set").build());
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final TextExtractionLimitReachedException[] limitReached = new TextExtractionLimitReachedException[1];
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                final Writer writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
                final LimitedTextContentHandler handler = new LimitedTextContentHandler(writer, charactersLimit, sizeLimit);
                try {
                    parser.parse(in, new BodyContentHandler(handler), new Metadata(), new ParseContext());
                } catch (final ZeroByteFileException e) {
                    getLogger().debug("Zero byte content, extracted text is empty");
                } catch (final EncryptedDocumentException e) {
                    throw new EncryptedOrPasswordProtectedFileException("Encrypted or Password Protected File Detected", e);
                } catch (final UnsupportedFormatException e) {
                    throw new UnsupportedFileType("Provided file type not supported yet", e);
                } catch (final SAXException | TikaException e) {
                    if (handler.getLimitReached() == null) {
                        throw new ExtractionException(e);
                    }
                } catch (final IOException e) {
                    throw new ExtractionException(e);
                }
                writer.flush();
                limitReached[0] = handler.getLimitReached();
                if (limitReached[0] != null && failOnLimitReached) {
                    // discards the partially written content; the FlowFile keeps its original content
                    throw new ExtractionException(limitReached[0]);
                }
            });
        } catch (final Exception e) {
            final Throwable cause = e instanceof ExtractionException ? e.getCause() : e;
            final Map<String, String> attributes = exceptionAttributes(cause);
            if (cause instanceof TextExtractionLimitReachedException) {
                attributes.put(TEXT_EXTRACTION_LIMIT_REACHED, String.valueOf(true));
                getLogger().warn("Text extraction limit reached for {}: {}", flowFile, cause.getMessage());
            } else {
                getLogger().error("Failed to extract text from {}", flowFile, cause);
            }
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(DDCoreAttributes.MIME_TYPE, TEXT_PLAIN);
        if (limitReached[0] != null) {
            attributes.putAll(exceptionAttributes(limitReached[0]));
            attributes.put(TEXT_EXTRACTION_LIMIT_REACHED, String.valueOf(true));
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().modifyContent(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * Carries a checked exception (or the limit exception) out of the stream callback.
     */
    private static final class ExtractionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ExtractionException(final Throwable cause) {
            super(cause);
        }
    }
}
