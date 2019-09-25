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
package io.siddhi.extension.map.binary.utils;

import io.siddhi.query.api.definition.Attribute;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Util helping to convert from Siddhi Event to byte message.
 */
public final class BinaryMessageConverterUtil {

    private BinaryMessageConverterUtil() {

    }

    public static byte[] loadData(InputStream in, byte[] dataArray) throws IOException {

        int start = 0;
        while (true) {
            int readCount = in.read(dataArray, start, dataArray.length - start);
            if (readCount != -1) {
                start += readCount;
                if (start == dataArray.length) {
                    return dataArray;
                }
            } else {
                throw new EOFException("Connection closed from remote end.");
            }
        }
    }

    public static String getString(ByteBuffer byteBuffer, int size) throws UnsupportedEncodingException {

        byte[] bytes = new byte[size];
        byteBuffer.get(bytes);
        return new String(bytes, Charset.defaultCharset());
    }

    public static int getSize(Object data, Attribute.Type type) {
        switch (type) {

            case STRING:
                return 4 + ((String) data).getBytes(Charset.defaultCharset()).length;
            case INT:
                return 4;
            case LONG:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            case BOOL:
                return 1;
            default:
                return 4;
        }
    }

    public static void assignData(Object data, ByteBuffer eventDataBuffer, Attribute.Type type) throws IOException {
        switch (type) {

            case STRING:
                eventDataBuffer.putInt(((String) data).getBytes(Charset.defaultCharset()).length);
                eventDataBuffer.put((((String) data).getBytes(Charset.defaultCharset())));
                break;
            case INT:
                eventDataBuffer.putInt((Integer) data);
                break;
            case LONG:
                eventDataBuffer.putLong((Long) data);
                break;
            case FLOAT:
                eventDataBuffer.putFloat((Float) data);
                break;
            case DOUBLE:
                eventDataBuffer.putDouble((Double) data);
                break;
            case BOOL:
                eventDataBuffer.put((byte) (((Boolean) data) ? 1 : 0));
                break;
            case OBJECT:
                eventDataBuffer.putInt(0);
                break;
        }

    }
}
