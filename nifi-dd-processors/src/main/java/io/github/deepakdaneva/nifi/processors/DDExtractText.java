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
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
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
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.UnsupportedFormatException;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
@Tags({DDCoreAttributes.DEEPAKDANEVA, "text", "content", "extraction", "tika"})
@CapabilityDescription("Parses the FlowFile and extract the text content. NOTE: This processor uses Apache Tika NOTE: This processor is backed by Apache Tika (https://tika.apache.org) check this to know more.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.MIME_TYPE, description = "If text content is extracted successfully, then this attribute's value will be set to 'text/plain'."), @WritesAttribute(attribute = DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, description = "This attribute will be set to outgoing FlowFiles (either failure or success) if there is more content to extract than the specified size or characters limit. If there's no breach of any limit then this attribute will not be set and won't be available on outgoing FlowFiles."), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
public class DDExtractText extends AbstractProcessor {

    public static final String TEXT_EXTRACTION_LIMIT_REACHED = "text.extraction.limit.reached";
    public static final PropertyDescriptor EXTRACT_SIZE_LIMIT = new PropertyDescriptor.Builder().name("Text extraction size limit").description("Specifies the maximum size of text content upto which text should be extracted. Set -1 for no size limit.").required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("80 MB").build();
    public static final PropertyDescriptor EXTRACT_CHARACTERS_LIMIT = new PropertyDescriptor.Builder().name("Text extraction characters limit").description("Specifies the maximum number of characters upto which text content should be extracted. Set -1 for no characters limit.").required(true).addValidator(StandardValidators.INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("100000").build();
    public static final PropertyDescriptor FAIL_ON_LIMIT_REACHED = new PropertyDescriptor.Builder().name("Fail on limit reached").description("If true and Characters limit or Extract Size limit reached will route the FlowFile to failure or else will route the FlowFile to success with the extracted content under the specified limit. Attribute '" + TEXT_EXTRACTION_LIMIT_REACHED + "' will be set irrespective of where FlowFile is routed when limit reached.").required(true).allowableValues("true", "false").defaultValue("true").build();
    public static final PropertyDescriptor TIKA_CONFIG_FILE = new PropertyDescriptor.Builder().name("Tika Config File").required(false).description("Path to Tika config file. Only one of Config File or Config Body may be used.").addValidator(new StandardValidators.FileExistsValidator(true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
    public static final PropertyDescriptor TIKA_CONFIG_BODY = new PropertyDescriptor.Builder().name("Tika Config Body").required(false).description("Body of Tika config file. Only one of Config File or Config Body may be used.").addValidator(Validator.VALID).expressionLanguageSupported(ExpressionLanguageScope.NONE).build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Successfully extracted text content will be transferred to this relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Failed FlowFiles will be transferred to this relationship").build();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private long ALLOWED_SIZE_LIMIT;
    private int ALLOWED_CHARACTERS_LIMIT;
    private boolean TO_FAILURE_ON_LIMIT_REACHED;
    private Parser parser;

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = List.of(EXTRACT_SIZE_LIMIT, EXTRACT_CHARACTERS_LIMIT, FAIL_ON_LIMIT_REACHED, TIKA_CONFIG_FILE, TIKA_CONFIG_BODY);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        PropertyValue pv = context.getProperty(EXTRACT_SIZE_LIMIT).evaluateAttributeExpressions();
        if (pv.getValue() != null && pv.getValue().trim().equals("-1")) {
            ALLOWED_SIZE_LIMIT = -1L;
        } else {
            ALLOWED_SIZE_LIMIT = pv.asDataSize(DataUnit.B).longValue();
        }
        ALLOWED_CHARACTERS_LIMIT = context.getProperty(EXTRACT_CHARACTERS_LIMIT).evaluateAttributeExpressions().asInteger();
        TO_FAILURE_ON_LIMIT_REACHED = context.getProperty(FAIL_ON_LIMIT_REACHED).asBoolean();

        String configBody = context.getProperty(TIKA_CONFIG_BODY).getValue();
        String configFile = context.getProperty(TIKA_CONFIG_FILE).evaluateAttributeExpressions().getValue();
        if (configBody == null && configFile == null) {
            parser = new AutoDetectParser(TikaConfig.getDefaultConfig());
        } else if (configBody != null) {
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(configBody.getBytes()); final BufferedInputStream bis = new BufferedInputStream(bais)) {
                parser = new AutoDetectParser(new TikaConfig(bis));
            } catch (Exception e) {
                parser = null;
                context.yield();
                throw new ProcessException("Failed to load Tika config body", e);
            }
        } else {
            try (final FileInputStream fis = new FileInputStream(configFile); final InputStream bis = new BufferedInputStream(fis)) {
                parser = new AutoDetectParser(new TikaConfig(bis));
            } catch (Exception e) {
                parser = null;
                context.yield();
                throw new ProcessException("Failed to load Tika config file", e);
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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog log = getLogger();
        InputStream in = null;
        try {
            in = session.read(flowFile);
            ContentHandler ch = new LimitedBodyContentHandler(ALLOWED_CHARACTERS_LIMIT, ALLOWED_SIZE_LIMIT);
            Metadata md = new Metadata();
            ParseContext pc = new ParseContext();
            try {
                parser.parse(in, ch, md, pc);
            } catch (ZeroByteFileException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Zero byte file exception", e);
                }
            } catch (EncryptedDocumentException e) {
                IOUtils.closeQuietly(in);
                throw new EncryptedOrPasswordProtectedFileException("Encrypted or Password Protected File Detected", e);
            } catch (UnsupportedFormatException e) {
                IOUtils.closeQuietly(in);
                throw new UnsupportedFileType("Provided file type not supported yet", e);
            } catch (TextExtractionLimitReachedException e) {
                IOUtils.closeQuietly(in);
                final Map<String, String> m = Map.of(DDExtractText.TEXT_EXTRACTION_LIMIT_REACHED, String.valueOf(true), DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, e.getClass().getName(), DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, e.getMessage());
                flowFile = session.putAllAttributes(flowFile, m);
                if (TO_FAILURE_ON_LIMIT_REACHED) {
                    throw e;
                }
            } finally {
                IOUtils.closeQuietly(in);
            }
            flowFile = session.write(flowFile, out -> IOUtils.write(ch.toString().trim(), out, Charset.defaultCharset()));
            flowFile = session.putAttribute(flowFile, DDCoreAttributes.MIME_TYPE, "text/plain");
            session.getProvenanceReporter().modifyContent(flowFile);
            route(session, flowFile, null, REL_SUCCESS);
        } catch (Exception e) {
            IOUtils.closeQuietly(in);
            route(session, flowFile, e, REL_FAILURE);
            log.error("Error while extracting text", e);
        }
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

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        String sizeLimit = validationContext.getProperty(EXTRACT_SIZE_LIMIT).evaluateAttributeExpressions().getValue();
        String body = validationContext.getProperty(TIKA_CONFIG_BODY).getValue();
        String file = validationContext.getProperty(TIKA_CONFIG_FILE).evaluateAttributeExpressions().getValue();
        if (sizeLimit != null && !"-1".equals(sizeLimit.trim())) {
            ValidationResult vr = StandardValidators.DATA_SIZE_VALIDATOR.validate(EXTRACT_SIZE_LIMIT.getName(), sizeLimit, validationContext);
            if (!vr.isValid()) {
                results.add(vr);
            }
        }
        if (body != null && file != null) {
            results.add(new ValidationResult.Builder().input(TIKA_CONFIG_FILE.getName()).subject(file).valid(false).explanation("can only specify '" + TIKA_CONFIG_BODY.getName() + "' or '" + TIKA_CONFIG_BODY.getName() + "'. Not both.").build());
        }
        return results;
    }
}

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
class LimitedBodyContentHandler extends BodyContentHandler {

    public LimitedBodyContentHandler(ContentHandler handler) {
        super(handler);
    }

    public LimitedBodyContentHandler(int charsWriteLimit, long bytesWriteLimit) {
        this(new LimitedWriteOutContentHandler(charsWriteLimit, bytesWriteLimit));
    }

}

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
class LimitedWriteOutContentHandler extends WriteOutContentHandler {

    /**
     * The maximum number of characters to write to the character stream.
     * Set to -1 for no limit.
     */
    private final int charsWriteLimit;

    /**
     * The maximum number of bytes to write to the stream.
     * Set to -1 for no limit.
     */
    private final long writeLimit;

    /**
     * bytes written so far.
     */
    private long writeCount = 0L;

    private boolean writeLimitReached;

    public LimitedWriteOutContentHandler(final int charsWriteLimit, final long bytesWriteLimit) {
        super(charsWriteLimit);
        this.charsWriteLimit = charsWriteLimit;
        this.writeLimit = bytesWriteLimit;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (writeLimitReached) {
            return;
        }
        int singleCharByte = String.valueOf(ch[0]).getBytes().length;
        int newBytesSize = singleCharByte * length;
        if (writeLimit == -1 || writeCount + newBytesSize <= writeLimit) {
            try {
                super.characters(ch, start, length);
            } catch (WriteLimitReachedException e) {
                writeLimitReached = true;
                throw new TextExtractionLimitReachedException(charsWriteLimit);
            }
            writeCount += newBytesSize;
        } else {
            // since limit is reached now only extract upto these many characters under this number of bytes
            int charsToExtract = ((int) (writeLimit - writeCount)) / singleCharByte;
            try {
                super.characters(ch, start, Math.min(charsToExtract, length));
            } catch (WriteLimitReachedException ignored) {
                // As we are already in the code block to handle the size limit reach exception we are ignoring this exception
            }
            handleWriteLimitReached();
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (writeLimitReached) {
            return;
        }
        int singleCharByte = String.valueOf(ch[0]).getBytes().length;
        int newBytesSize = singleCharByte * length;
        if (writeLimit == -1 || writeCount + newBytesSize <= writeLimit) {
            try {
                super.ignorableWhitespace(ch, start, length);
            } catch (WriteLimitReachedException e) {
                writeLimitReached = true;
                throw new TextExtractionLimitReachedException(charsWriteLimit);
            }
            writeCount += newBytesSize;
        } else {
            // since limit is reached now only extract upto these many characters under this number of bytes
            int charsToExtract = ((int) (writeLimit - writeCount)) / singleCharByte;
            try {
                super.ignorableWhitespace(ch, start, Math.min(charsToExtract, length));
            } catch (WriteLimitReachedException ignored) {
                // As we are already in the code block to handle the size limit reach exception we are ignoring this exception
            }
            handleWriteLimitReached();
        }
    }

    private void handleWriteLimitReached() throws TextExtractionLimitReachedException {
        writeLimitReached = true;
        writeCount = writeLimit;
        throw new TextExtractionLimitReachedException(writeLimit);
    }
}
