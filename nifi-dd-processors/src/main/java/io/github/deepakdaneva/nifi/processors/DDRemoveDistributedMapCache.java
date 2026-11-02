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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Removes an entry from a map cache through a {@link DistributedMapCacheClient}.
 *
 * @author Deepak Kumar Jangir
 * @since 1.0.0
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "map", "cache", "remove", "delete", "distributed"})
@CapabilityDescription("Removes the entry identified by 'Cache Entry Identifier' from the map cache behind the configured DistributedMapCacheClient (for example MapCacheClientService, " + "or a Redis or Hazelcast map cache client). The cache key is serialized as UTF-8, the same way PutDistributedMapCache and FetchDistributedMapCache serialize it.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.MapCacheClientService", "org.apache.nifi.distributed.cache.server.map.MapCacheServer", "org.apache.nifi.processors.standard.PutDistributedMapCache", "org.apache.nifi.processors.standard.FetchDistributedMapCache"})
public class DDRemoveDistributedMapCache extends AbstractDDProcessor {

    /**
     * Map cache client used to remove the entry.
     */
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder().name("Distributed Cache Service").description("The map cache client used to remove the cache entry.").required(true).identifiesControllerService(DistributedMapCacheClient.class).build();

    /**
     * Key of the entry to remove.
     */
    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder().name("Cache Entry Identifier").description("Key of the cache entry to remove. Supports Expression Language with FlowFile attributes; a blank result routes the FlowFile to 'failure'.").required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /**
     * Relationship for removed entries.
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("The cache entry was removed.").build();
    /**
     * Relationship for keys not present in the cache.
     */
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not-found").description("No cache entry exists for the evaluated identifier.").build();
    /**
     * Relationship for failures.
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("The cache service could not be reached or the identifier evaluated to a blank value. The FlowFile is penalized.").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(CACHE_ENTRY_IDENTIFIER, DISTRIBUTED_CACHE_SERVICE);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_NOT_FOUND, REL_FAILURE);
    private static final Serializer<String> KEY_SERIALIZER = new StringSerializer();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        if (cacheKey == null || cacheKey.isBlank()) {
            fail(session, flowFile, new NoOrBlankCacheIdentifierException("Empty/Blank/No cache identifier provided"));
            return;
        }

        final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        final boolean removed;
        try {
            removed = cache.remove(cacheKey, KEY_SERIALIZER);
        } catch (final IOException | RuntimeException e) {
            fail(session, flowFile, e);
            return;
        }
        session.transfer(flowFile, removed ? REL_SUCCESS : REL_NOT_FOUND);
    }

    private void fail(final ProcessSession session, final FlowFile flowFile, final Exception e) {
        getLogger().error("Failed to remove cache entry for {}", flowFile, e);
        transfer(session, session.penalize(flowFile), REL_FAILURE, e);
    }

    /**
     * Serializes cache keys as UTF-8 bytes.
     */
    public static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream out) throws IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
