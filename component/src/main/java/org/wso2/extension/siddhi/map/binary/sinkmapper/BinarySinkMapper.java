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

package org.wso2.extension.siddhi.map.binary.sinkmapper;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.util.Arrays;

/**
 * Binary sink mapper extension.
 */
@Extension(
        name = "binary",
        namespace = "sinkMapper",
        description = "TBD",
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class BinarySinkMapper extends SinkMapper {

    private static final Logger LOG = Logger.getLogger(BinarySinkMapper.class);
    private StreamDefinition streamDefinition;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{};
    }

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, TemplateBuilder templateBuilder,
                     ConfigReader configReader) {

        this.streamDefinition = streamDefinition;
        if (templateBuilder != null) {
            LOG.error("Binary SinkMapper does not support @payload mapping, error at the mapper of " +
                    streamDefinition.getId());
        }
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, TemplateBuilder templateBuilder, SinkListener
            sinkListener, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
        try {
            sinkListener.publish(BinaryEventConverter.convertToBinaryMessage(events), dynamicOptions);
        } catch (IOException e) {
            LOG.error("Error in converting event '" + Arrays.deepToString(events) + "' to binary format at binary " +
                    "SinkMapper of " +
                    streamDefinition.getId());
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, TemplateBuilder templateBuilder, SinkListener
            sinkListener, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
        mapAndSend(new Event[]{event}, optionHolder, templateBuilder, sinkListener, dynamicOptions);
    }


}
