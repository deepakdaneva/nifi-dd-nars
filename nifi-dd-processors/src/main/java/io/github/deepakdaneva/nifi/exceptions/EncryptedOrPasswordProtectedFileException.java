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

/**
 * Raised when a document cannot be parsed because it is encrypted or password protected.
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
public class EncryptedOrPasswordProtectedFileException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message description of the problem.
     * @param throwable underlying cause.
     */
    public EncryptedOrPasswordProtectedFileException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
