/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.map.binary;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;


/**
 * TCP sinkmapper test case.
 */
public class BinarySourceMapperTestCase {
    static final Logger LOG = Logger.getLogger(BinarySourceMapperTestCase.class);
    private volatile int count;
    private volatile int count1;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        count1 = 0;
        eventArrived = false;
    }

    @Test
    public void binarySourceMapperTest1() throws InterruptedException, IOException {
        LOG.info("binary SourceMapper TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@source(type='inMemory', topic='WSO2', @map(type='binary'))\n" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "" +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        InMemoryBroker.publish("WSO2", ByteBuffer.wrap(BinaryEventConverter.convertToBinaryMessage(
                arrayList.toArray(new Event[3]), types).array()));

        Thread.sleep(300);

        siddhiAppRuntime.shutdown();

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
    }

    @Test(dependsOnMethods = "binarySourceMapperTest1")
    public void binarySourceMapperTest2() throws InterruptedException, IOException {
        LOG.info("binary SourceMapper TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@source(type='inMemory', topic='WSO2', @map(type='binary'))\n" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "" +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());
        types[1] = Attribute.Type.FLOAT;

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            AssertJUnit.assertTrue((Integer) event.getData(1) != 36f);
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36f, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361f, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362f, 32.0f, 3802L, 232.0, true}));
        InMemoryBroker.publish("WSO2", ByteBuffer.wrap(BinaryEventConverter.convertToBinaryMessage(
                arrayList.toArray(new Event[3]), types).array()));

        Thread.sleep(300);

        siddhiAppRuntime.shutdown();

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
    }

    @Test(dependsOnMethods = "binarySourceMapperTest2")
    public void binarySourceMapperTest3() throws InterruptedException, IOException {
        LOG.info("binary SourceMapper TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@source(type='inMemory', topic='WSO2', @map(type='binary'))\n" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "" +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());
        types[1] = Attribute.Type.DOUBLE;

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            AssertJUnit.assertTrue((Integer) event.getData(1) != 36f);
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36.0, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361.0, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362.0, 32.0f, 3802L, 232.0, true}));
        InMemoryBroker.publish("WSO2", ByteBuffer.wrap(BinaryEventConverter.convertToBinaryMessage(
                arrayList.toArray(new Event[3]), types).array()));

        Thread.sleep(300);

        siddhiAppRuntime.shutdown();

        AssertJUnit.assertEquals(0, count);
        AssertJUnit.assertFalse(eventArrived);
    }
}

