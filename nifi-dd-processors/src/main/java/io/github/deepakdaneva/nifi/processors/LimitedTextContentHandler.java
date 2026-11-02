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

import io.github.deepakdaneva.nifi.exceptions.TextExtractionLimitReachedException;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.Writer;

/**
 * SAX handler that writes character content to a {@link Writer} while enforcing a character limit and a UTF-8 byte
 * limit. Leading and trailing whitespace (characters {@code <= ' '}) of the whole document is dropped, the same way
 * {@link String#trim()} would, without buffering the document in memory.
 * <p>
 * When a limit is reached, the text up to the limit is written and {@link TextExtractionLimitReachedException} is
 * thrown; it extends Tika's {@code WriteLimitReachedException} so parsers propagate it instead of swallowing it.
 */
final class LimitedTextContentHandler extends DefaultHandler {

    private final Writer writer;
    private final long charLimit;
    private final long byteLimit;
    private final StringBuilder pendingWhitespace = new StringBuilder();
    private long chars;
    private long bytes;
    private boolean started;
    private TextExtractionLimitReachedException limitReached;

    /**
     * @param writer destination of the text.
     * @param charLimit maximum number of characters, negative for no limit.
     * @param byteLimit maximum number of UTF-8 bytes, negative for no limit.
     */
    LimitedTextContentHandler(final Writer writer, final long charLimit, final long byteLimit) {
        this.writer = writer;
        this.charLimit = charLimit;
        this.byteLimit = byteLimit;
    }

    /**
     * @return the limit exception if a limit was reached, otherwise {@code null}.
     */
    TextExtractionLimitReachedException getLimitReached() {
        return limitReached;
    }

    @Override
    public void characters(final char[] ch, final int start, final int length) throws SAXException {
        append(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(final char[] ch, final int start, final int length) throws SAXException {
        append(ch, start, length);
    }

    private void append(final char[] ch, final int start, final int length) throws SAXException {
        if (limitReached != null) {
            throw limitReached;
        }
        int end = start;
        final int limitEnd = start + length;
        TextExtractionLimitReachedException reached = null;
        while (end < limitEnd) {
            if (charLimit >= 0 && chars >= charLimit) {
                reached = new TextExtractionLimitReachedException((int) Math.min(charLimit, Integer.MAX_VALUE));
                break;
            }
            final char c = ch[end];
            final boolean pair = Character.isHighSurrogate(c) && end + 1 < limitEnd && Character.isLowSurrogate(ch[end + 1]);
            final int charCount = pair ? 2 : 1;
            final int byteCount = pair ? 4 : utf8Length(c);
            if (byteLimit >= 0 && bytes + byteCount > byteLimit) {
                reached = new TextExtractionLimitReachedException(byteLimit);
                break;
            }
            if (charLimit >= 0 && chars + charCount > charLimit) {
                reached = new TextExtractionLimitReachedException((int) Math.min(charLimit, Integer.MAX_VALUE));
                break;
            }
            chars += charCount;
            bytes += byteCount;
            end += charCount;
        }
        write(ch, start, end - start);
        if (reached != null) {
            limitReached = reached;
            throw reached;
        }
    }

    private void write(final char[] ch, final int start, final int length) throws SAXException {
        try {
            int runStart = start;
            for (int i = start; i < start + length; i++) {
                if (ch[i] <= ' ') {
                    if (i > runStart) {
                        writer.write(ch, runStart, i - runStart);
                    }
                    if (started) {
                        pendingWhitespace.append(ch[i]);
                    }
                    runStart = i + 1;
                } else if (pendingWhitespace.length() > 0) {
                    writer.append(pendingWhitespace);
                    pendingWhitespace.setLength(0);
                    started = true;
                } else {
                    started = true;
                }
            }
            if (start + length > runStart) {
                writer.write(ch, runStart, start + length - runStart);
            }
        } catch (final IOException e) {
            throw new SAXException("Failed to write extracted text", e);
        }
    }

    private static int utf8Length(final char c) {
        if (c < 0x80) {
            return 1;
        } else if (c < 0x800) {
            return 2;
        }
        // BMP characters and unpaired surrogates (encoded as '?' replacement, counted conservatively)
        return 3;
    }
}
