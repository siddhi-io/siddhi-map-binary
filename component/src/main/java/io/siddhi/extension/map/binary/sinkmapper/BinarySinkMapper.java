/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.map.binary.sinkmapper;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.extension.map.binary.utils.EventDefinitionConverterUtil;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 * Binary sink mapper extension.
 */
@Extension(
        name = "binary",
        namespace = "sinkMapper",
        description = "This section explains how to map events processed via Siddhi in order to publish them in " +
                "the `binary` format.",
        examples = @Example(syntax = "@sink(type='inMemory', topic='WSO2', @map(type='binary')) " +
                "define stream FooStream (symbol string, price float, volume long); ",
                description = "This will publish Siddhi event in binary format.")
)
public class BinarySinkMapper extends SinkMapper {

    private static final Logger LOG = Logger.getLogger(BinarySinkMapper.class);
    private StreamDefinition streamDefinition;
    private Attribute.Type[] types;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
            Map<String, TemplateBuilder> payloadTemplateBuilderMap, ConfigReader configReader,
            SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.types = EventDefinitionConverterUtil.generateAttributeTypeArray(streamDefinition.getAttributeList());
        if (payloadTemplateBuilderMap != null) {
            throw new SiddhiAppCreationException("Binary sink-mapper does not support @payload mapping, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{};
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{ByteBuffer.class};
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder,
            Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        try {
            sinkListener.publish(BinaryEventConverter.convertToBinaryMessage(events, types));
        } catch (Throwable e) {
            LOG.error("Error in converting event '" + Arrays.deepToString(events) +
                    "' to binary format at binary SinkMapper of " + streamDefinition.getId(), e);
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
            Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        mapAndSend(new Event[]{event}, optionHolder, payloadTemplateBuilderMap, sinkListener);
    }

}
