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
package io.github.deepakdaneva.nifi.exceptions;

import org.apache.tika.exception.WriteLimitReachedException;

/**
 * Raised when a document holds more text than the configured characters or size limit. Extends Tika's
 * {@link WriteLimitReachedException} so that Tika parsers propagate it instead of recording it as an embedded document
 * failure.
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
public class TextExtractionLimitReachedException extends WriteLimitReachedException {

    private static final long serialVersionUID = 1L;

    private final String message;

    /**
     * Creates the exception for a characters limit.
     *
     * @param charsLimit characters limit that was reached.
     */
    public TextExtractionLimitReachedException(final int charsLimit) {
        super(charsLimit);
        this.message = "Your document contained more than " + charsLimit + " characters, and so your requested limit has been reached. To receive the full text of the document, increase your limit. (Text up to the limit is however available).";
    }

    /**
     * Creates the exception for a size limit.
     *
     * @param sizeLimit size limit in bytes that was reached.
     */
    public TextExtractionLimitReachedException(final long sizeLimit) {
        super(0);
        this.message = "Your document contained more than " + sizeLimit + " bytes, and so your requested limit has been reached. To receive the full text of the document, increase your limit. (Text up to the limit is however available).";
    }

    @Override
    public String getMessage() {
        return message;
    }
}
