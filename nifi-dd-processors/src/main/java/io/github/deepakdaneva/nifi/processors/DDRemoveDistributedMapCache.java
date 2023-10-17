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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * @author Deepak Kumar Jangir
 * @version 1
 * @since 1
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({DDCoreAttributes.DEEPAKDANEVA, "map", "cache", "remove", "distributed"})
@CapabilityDescription("Removes the provided key from the underlying DistributedMapCache.")
@WritesAttributes({@WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, description = DDCoreAttributes.DD_EXCEPTION_MESSAGE_DESC), @WritesAttribute(attribute = DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, description = DDCoreAttributes.DD_EXCEPTION_CLASS_DESC)})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer", "org.apache.nifi.processors.standard.PutDistributedMapCache"})
public class DDRemoveDistributedMapCache extends AbstractProcessor {

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder().name("Distributed Cache Service").description("The Controller Service that is used to remove the cached values.").required(true).identifiesControllerService(DistributedMapCacheClient.class).build();

    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder().name("Cache Entry Identifier").description("A cache identifier which needs to be removed from the cache.").required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR).addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).defaultValue("").expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("If the cache is successfully removed FlowFile will will be routed to this relationship.").build();
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not-found").description("If the cache is not found in the cache service with the provided identifier, FlowFile will be routed to this relationship.").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("If unable to communicate with the cache service or if the cache entry is evaluated to be blank, the FlowFile will be penalized and routed to this relationship.").build();
    private final Serializer<String> keySerializer;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public DDRemoveDistributedMapCache() {
        keySerializer = new StringSerializer();
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = List.of(CACHE_ENTRY_IDENTIFIER, DISTRIBUTED_CACHE_SERVICE);
        relationships = Set.of(REL_SUCCESS, REL_NOT_FOUND, REL_FAILURE);
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
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        if (!StringUtils.isBlank(cacheKey)) {
            DistributedMapCacheClient cache;
            try {
                cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
                if (cache.remove(cacheKey, keySerializer)) {
                    session.transfer(flowFile, REL_SUCCESS);
                } else {
                    session.transfer(flowFile, REL_NOT_FOUND);
                }
            } catch (Exception e) {
                log.error("Unable to remove the cache '" + cacheKey + "'", e);
                flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, e.getMessage());
                flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, e.getClass().getName());
                flowFile = session.penalize(flowFile);
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        } else {
            try {
                throw new NoOrBlankCacheIdentifierException("Empty/Blank/No cache identifier provided");
            } catch (Exception e) {
                log.error("Unable to remove the cache", e);
                flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_MESSAGE_KEY, e.getMessage());
                flowFile = session.putAttribute(flowFile, DDCoreAttributes.DD_EXCEPTION_CLASS_KEY, e.getClass().getName());
                flowFile = session.penalize(flowFile);
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * Simple string serializer, used for serializing the cache key
     */
    public static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
