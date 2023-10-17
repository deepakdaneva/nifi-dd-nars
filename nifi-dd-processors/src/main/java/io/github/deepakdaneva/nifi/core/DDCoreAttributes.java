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
package io.github.deepakdaneva.nifi.core;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public class DDCoreAttributes {
    /**
     * Deepak Daneva
     */
    public static final String DEEPAKDANEVA = "deepakdaneva";
    /**
     * A unique UUID assigned to the FlowFile.
     */
    public static final String UUID = "uuid";
    /**
     * The FlowFile's path indicates the directory to which a FlowFile belongs and does not contain the filename.
     */
    public static final String PATH = "path";
    /**
     * The filename of the FlowFile. The filename should not contain any directory structure.
     */
    public static final String FILENAME = "filename";
    /**
     * The FlowFile's absolute path indicates the absolute directory to which a FlowFile belongs and does not contain the filename.
     */
    public static final String ABSOLUTE_PATH = "absolute.path";
    /**
     * The MIME Type of the FlowFile.
     */
    public static final String MIME_TYPE = "mime.type";
    /**
     * Indicates an identifier other than the FlowFile's UUID that is known to refer to this FlowFile.
     */
    public static final String ALTERNATE_IDENTIFIER = "alternate.identifier";
    /**
     * The exception message attribute of any exception raised during the processor execution.
     */
    public static final String DD_EXCEPTION_MESSAGE_KEY = DEEPAKDANEVA + ".exception.message";
    /**
     * The exception message attribute description of any exception raised during the processor execution.
     */
    public static final String DD_EXCEPTION_MESSAGE_DESC = "If any exception is thrown during processor execution then exception message will be provided through this attribute.";
    /**
     * The exception class name attribute of any exception raised during the processor execution.
     */
    public static final String DD_EXCEPTION_CLASS_KEY = DEEPAKDANEVA + ".exception.class";
    /**
     * The exception class name attribute description of any exception raised during the processor execution.
     */
    public static final String DD_EXCEPTION_CLASS_DESC = "If any exception is thrown during processor execution then full exception class name will be provided through this attribute.";

    public static class DDFragmentAttributes {
        /**
         * All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute.
         */
        public static final String FRAGMENT_IDENTIFIER = "fragment.identifier";
        /**
         * A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile.
         */
        public static final String FRAGMENT_INDEX = "fragment.index";
        /**
         * The number of split FlowFiles generated from the parent FlowFile.
         */
        public static final String FRAGMENT_COUNT = "fragment.count";
        /**
         * The filename of the parent FlowFile.
         */
        public static final String SEGMENT_ORIGINAL_FILENAME = "segment.original.filename";
        /**
         * The path of the FlowFile relative to the parent FlowFile.
         */
        public static final String SEGMENT_PATH = "segment.path";
    }
}
