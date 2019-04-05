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

package org.wso2.extension.siddhi.map.binary.sourcemapper;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;

import java.nio.ByteBuffer;
import java.util.List;

import static org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter.LOG;
import static org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter.toConvertToSiddhiEvents;

/**
 * Binary source mapper extension.
 */
@Extension(
        name = "binary",
        namespace = "sourceMapper",
        description = "This extension is a binary input mapper that converts events received in `binary`" +
                " format to Siddhi events before they are processed.",
        examples = @Example(syntax = "@source(type='inMemory', topic='WSO2', @map(type='binary'))" +
                "define stream FooStream (symbol string, price float, volume long); ",
                description = "This query performs a mapping to convert an event of the `binary` " +
                        "format to a Siddhi event. ")
)
public class BinarySourceMapper extends SourceMapper {

    private Attribute.Type[] types;
    private StreamDefinition streamDefinition;

    @Override
    public void init(StreamDefinition streamDefinition,
                     OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappings,
                     ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        types = EventDefinitionConverterUtil.generateAttributeTypeArray(streamDefinition
                .getAttributeList());
        if (attributeMappings != null && attributeMappings.size() > 0) {
            throw new SiddhiAppCreationException("'binary' source-mapper does not support custom mapping, " +
                    "but found at stream '" + streamDefinition.getId() + "'");
        }
        this.streamDefinition = streamDefinition;
    }

    @Override
    protected void mapAndProcess(Object o, InputEventHandler inputEventHandler) throws InterruptedException {
        try {
            if (o != null) {
                Event[] events;
                if (o instanceof ByteBuffer) {
                    events = toConvertToSiddhiEvents((ByteBuffer) o, types);
                } else {
                    events = toConvertToSiddhiEvents(ByteBuffer.wrap((byte[]) o), types);
                }
                if (events != null) {
                    inputEventHandler.sendEvents(events);
                }
            }
        } catch (Throwable t) {
            LOG.error("Error at binary source mapper of '" + streamDefinition.getId() + "' " + t.getMessage(), t);
        }
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return false;
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{ByteBuffer.class, byte[].class};
    }

}
