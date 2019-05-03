/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.map.binary.sinkmapper;

import io.siddhi.core.event.Event;
import io.siddhi.extension.map.binary.utils.BinaryMessageConverterUtil;
import io.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This is a Util class help to convert from Siddhi event to Binary message.
 */
public class BinaryEventConverter {

    public static ByteBuffer convertToBinaryMessage(Event[] events, Attribute.Type[] types) throws IOException {

        int eventCount = events.length;

        int messageSize = 4;
        for (Event event : events) {
            messageSize += getEventSize(event, types);
        }

        ByteBuffer messageBuffer = ByteBuffer.wrap(new byte[messageSize]);
        messageBuffer.putInt(eventCount);

        for (Event event : events) {
            messageBuffer.putLong(event.getTimestamp());
            if (event.getData() != null && event.getData().length != 0) {
                Object[] data = event.getData();
                for (int i = 0; i < data.length; i++) {
                    Object aData = data[i];
                    BinaryMessageConverterUtil.assignData(aData, messageBuffer, types[i]);
                }
            }
        }

        return messageBuffer;

    }

    private static int getEventSize(Event event, Attribute.Type[] types) {
        int eventSize = 8;
        Object[] data = event.getData();
        if (data != null) {
            for (int i = 0; i < data.length; i++) {
                Object aData = data[i];
                eventSize += BinaryMessageConverterUtil.getSize(aData, types[i]);
            }
        }
        return eventSize;
    }

//    public static void main(String[] args) throws IOException {
//        for (int j = 0; j < 10; j++) {
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i < 10000000; i++) {
//                convertToBinaryMessage(new Event[]{new Event(System.currentTimeMillis(),
//                        new Object[]{"WSO2", i, 10}), new Event(System.currentTimeMillis(),
//                        new Object[]{"WSO2", i, 10})});
//            }
//        }
//    }
}
