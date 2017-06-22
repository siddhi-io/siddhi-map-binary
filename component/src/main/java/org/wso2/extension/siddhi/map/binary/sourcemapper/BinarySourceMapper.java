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

import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.AttributeMapping;
import org.wso2.siddhi.core.stream.input.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

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
        description = "TBD",
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class BinarySourceMapper extends SourceMapper {

    private Attribute.Type[] types;
    private StreamDefinition streamDefinition;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping> list,
                     ConfigReader configReader) {
        types = EventDefinitionConverterUtil.generateAttributeTypeArray(streamDefinition
                .getAttributeList());
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
}
