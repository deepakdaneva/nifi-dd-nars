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

import io.github.deepakdaneva.nifi.core.DDCoreAttributes;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Common plumbing shared by the processors of this bundle: exception attributes, routing and value parsing helpers.
 *
 * @author Deepak Kumar Jangir
 * @since 2.0.0
 */
public abstract class AbstractDDProcessor extends AbstractProcessor {

    /**
     * Value accepted by size/limit properties to disable the limit.
     */
    protected static final String UNLIMITED = "-1";

    /**
     * Sentinel returned by {@link #parseDataSizeOrUnlimited(PropertyValue)} when no limit applies.
     */
    protected static final long NO_LIMIT = -1L;

    /**
     * Generic binary MIME type, used when no better type is known.
     */
    protected static final String OCTET_STREAM = "application/octet-stream";

    /**
     * Validates a data size such as {@code 80 MB}, or {@value #UNLIMITED} for no limit. Expression Language is
     * evaluated against the environment before validation.
     */
    protected static final Validator DATA_SIZE_OR_UNLIMITED_VALIDATOR = (subject, input, context) -> {
        final String value = context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input) ? context.newPropertyValue(input).evaluateAttributeExpressions().getValue() : input;
        if (value != null && UNLIMITED.equals(value.trim())) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
        return StandardValidators.DATA_SIZE_VALIDATOR.validate(subject, value, context);
    };

    /**
     * Validates an integer that is either positive or {@value #UNLIMITED} for no limit.
     */
    protected static final Validator POSITIVE_INTEGER_OR_UNLIMITED_VALIDATOR = (subject, input, context) -> {
        final String value = context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input) ? context.newPropertyValue(input).evaluateAttributeExpressions().getValue() : input;
        if (value != null && UNLIMITED.equals(value.trim())) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
        return StandardValidators.POSITIVE_INTEGER_VALIDATOR.validate(subject, value, context);
    };

    /**
     * Parses a property validated by {@link #DATA_SIZE_OR_UNLIMITED_VALIDATOR}.
     *
     * @param value property value (Expression Language is evaluated against the environment).
     * @return the size in bytes, or {@link #NO_LIMIT}.
     */
    protected static long parseDataSizeOrUnlimited(final PropertyValue value) {
        final PropertyValue evaluated = value.evaluateAttributeExpressions();
        final String raw = evaluated.getValue();
        if (raw == null || UNLIMITED.equals(raw.trim())) {
            return NO_LIMIT;
        }
        return evaluated.asDataSize(DataUnit.B).longValue();
    }

    /**
     * Removes MIME type parameters, e.g. {@code application/x-rar-compressed; version=5} becomes
     * {@code application/x-rar-compressed}.
     *
     * @param mimeType MIME type, may be {@code null}.
     * @return the MIME type without parameters, or {@code null} if {@code mimeType} is {@code null}.
     */
    protected static String stripMimeParameters(final String mimeType) {
        if (mimeType == null) {
            return null;
        }
        final int separator = mimeType.indexOf(';');
        return (separator < 0 ? mimeType : mimeType.substring(0, separator)).trim();
    }

    /**
     * Detects the MIME type of a stream without consuming it.
     *
     * @param detector Tika detector.
     * @param inputStream stream supporting {@link InputStream#mark(int)}; it is reset after detection.
     * @return detected MIME type without parameters.
     * @throws IOException if the stream cannot be read.
     */
    protected static String detectMimeType(final Detector detector, final InputStream inputStream) throws IOException {
        return stripMimeParameters(detector.detect(inputStream, new Metadata()).toString());
    }

    /**
     * Builds the {@link DDCoreAttributes#DD_EXCEPTION_MESSAGE_KEY} and {@link DDCoreAttributes#DD_EXCEPTION_CLASS_KEY}
     * attributes for an exception.
     *
     * @param throwable exception; may be {@code null}, in which case an empty map is returned.
     * @return mutable attribute map.
     */
    protected static Map<String, String> exceptionAttributes(final Throwable throwable) {
        final Map<String, String> attributes = new HashMap<>();
        if (throwable != null) {
            final String message = throwable.getMessage();
            attributes.put(DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, message == null ? throwable.getClass().getSimpleName() : message);
            attributes.put(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, throwable.getClass().getName());
        }
        return attributes;
    }

    /**
     * Adds exception attributes (if {@code throwable} is not {@code null}) and transfers the FlowFile.
     *
     * @param session process session.
     * @param flowFile FlowFile to transfer.
     * @param relationship destination relationship.
     * @param throwable exception to describe on the FlowFile, or {@code null}.
     */
    protected void transfer(final ProcessSession session, FlowFile flowFile, final Relationship relationship, final Throwable throwable) {
        if (throwable != null) {
            flowFile = session.putAllAttributes(flowFile, exceptionAttributes(throwable));
            session.getProvenanceReporter().modifyAttributes(flowFile);
        }
        session.transfer(flowFile, relationship);
    }
}
