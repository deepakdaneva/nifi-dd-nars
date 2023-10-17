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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
public class TestDDRemoveDistributedMapCache {

    final String CACHE_KEY = "deepakdanevakey";
    final String CACHE_VALUE = "deepakdanevavalue";
    TestRunner runner;
    MockCacheClient cache;

    @BeforeEach
    public void putCacheKey() throws InitializationException, IOException {
        runner = TestRunners.newTestRunner(new DDRemoveDistributedMapCache());
        cache = new MockCacheClient(false);
        Serializer<String> serializer = new DDRemoveDistributedMapCache.StringSerializer();
        cache.put(CACHE_KEY, CACHE_VALUE, serializer, serializer);
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(DDRemoveDistributedMapCache.DISTRIBUTED_CACHE_SERVICE, "cache");
    }

    @Test
    public void test() {
        runner.setProperty(DDRemoveDistributedMapCache.CACHE_ENTRY_IDENTIFIER, CACHE_KEY);
        runner.enqueue(new byte[]{});
        runner.run();
        runner.assertAllFlowFilesTransferred(DDRemoveDistributedMapCache.REL_SUCCESS, 1);
        runner.assertTransferCount(DDRemoveDistributedMapCache.REL_SUCCESS, 1);
    }

    private static class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private final boolean failOnCalls;

        public MockCacheClient(final boolean failOnCalls) {
            this.failOnCalls = failOnCalls;
        }

        private void verifyNotFail() throws IOException {
            if (failOnCalls) {
                throw new IOException("Could not call to remote service because Unit Test marked service unavailable");
            }
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            final Object retValue = values.putIfAbsent(key, value);
            return (retValue == null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.putIfAbsent(key, value);
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            verifyNotFail();
            return values.containsKey(key);
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            values.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.get(key);
        }

        @Override
        public void close() {
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            verifyNotFail();
            values.remove(key);
            return true;
        }

        @Override
        public long removeByPattern(String regex) throws IOException {
            verifyNotFail();
            final List<Object> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (Object key : values.keySet()) {
                // Key must be backed by something that can be converted into a String
                Matcher m = p.matcher(key.toString());
                if (m.matches()) {
                    removedRecords.add(values.get(key));
                }
            }
            final long numRemoved = removedRecords.size();
            removedRecords.forEach(values::remove);
            return numRemoved;
        }
    }
}
