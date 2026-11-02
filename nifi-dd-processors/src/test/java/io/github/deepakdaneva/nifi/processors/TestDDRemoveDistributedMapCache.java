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
import io.github.deepakdaneva.nifi.exceptions.NoOrBlankCacheIdentifierException;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DDRemoveDistributedMapCache}.
 */
class TestDDRemoveDistributedMapCache {

    private static final String CACHE_KEY = "deepakdanevakey";
    private static final String CACHE_VALUE = "deepakdanevavalue";

    private TestRunner runner;
    private MockCacheClient cache;

    @BeforeEach
    void putCacheKey() throws InitializationException, IOException {
        runner = TestRunners.newTestRunner(new DDRemoveDistributedMapCache());
        cache = new MockCacheClient();
        final Serializer<String> serializer = new DDRemoveDistributedMapCache.StringSerializer();
        cache.put(CACHE_KEY, CACHE_VALUE, serializer, serializer);
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(DDRemoveDistributedMapCache.DISTRIBUTED_CACHE_SERVICE, "cache");
    }

    @Test
    void removesEntry() throws IOException {
        runner.setProperty(DDRemoveDistributedMapCache.CACHE_ENTRY_IDENTIFIER, CACHE_KEY);
        runner.enqueue(new byte[]{});
        runner.run();
        runner.assertAllFlowFilesTransferred(DDRemoveDistributedMapCache.REL_SUCCESS, 1);
        assertFalse(cache.containsKey(CACHE_KEY, new DDRemoveDistributedMapCache.StringSerializer()));
    }

    @Test
    void identifierFromAttribute() {
        runner.setProperty(DDRemoveDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${key}");
        runner.enqueue(new byte[]{}, Map.of("key", CACHE_KEY));
        runner.enqueue(new byte[]{}, Map.of("key", "missing"));
        runner.run(2);
        runner.assertTransferCount(DDRemoveDistributedMapCache.REL_SUCCESS, 1);
        runner.assertTransferCount(DDRemoveDistributedMapCache.REL_NOT_FOUND, 1);
    }

    @Test
    void blankIdentifierFails() {
        runner.setProperty(DDRemoveDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${missing}");
        runner.enqueue(new byte[]{});
        runner.run();
        runner.assertAllFlowFilesTransferred(DDRemoveDistributedMapCache.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDRemoveDistributedMapCache.REL_FAILURE).get(0);
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, NoOrBlankCacheIdentifierException.class.getName());
        assertTrue(ff.isPenalized());
    }

    @Test
    void cacheErrorFails() {
        cache.failOnCalls = true;
        runner.setProperty(DDRemoveDistributedMapCache.CACHE_ENTRY_IDENTIFIER, CACHE_KEY);
        runner.enqueue(new byte[]{});
        runner.run();
        runner.assertAllFlowFilesTransferred(DDRemoveDistributedMapCache.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(DDRemoveDistributedMapCache.REL_FAILURE).get(0);
        ff.assertAttributeEquals(DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, IOException.class.getName());
        assertTrue(ff.isPenalized());
    }

    @Test
    void identifierRequired() {
        runner.assertNotValid();
    }

    /**
     * In-memory cache keyed by the serialized key.
     */
    private static final class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<String, Object> values = new ConcurrentHashMap<>();
        private volatile boolean failOnCalls;

        private void verifyNotFail() throws IOException {
            if (failOnCalls) {
                throw new IOException("Could not call to remote service because Unit Test marked service unavailable");
            }
        }

        private static <K> String key(final K key, final Serializer<K> serializer) throws IOException {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializer.serialize(key, out);
            return out.toString(StandardCharsets.UTF_8);
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            return values.putIfAbsent(key(key, keySerializer), value) == null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.putIfAbsent(key(key, keySerializer), value);
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            verifyNotFail();
            return values.containsKey(key(key, keySerializer));
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            values.put(key(key, keySerializer), value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.get(key(key, keySerializer));
        }

        @Override
        public void close() {
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            verifyNotFail();
            return values.remove(key(key, serializer)) != null;
        }
    }
}
