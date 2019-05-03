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

package io.siddhi.extension.map.binary;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import io.siddhi.extension.map.binary.sinkmapper.BinaryEventConverter;
import io.siddhi.extension.map.binary.utils.EventDefinitionConverterUtil;
import io.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * TCP sinkmapper test case.
 */
public class BinarySourceMapperTestCase {
    private static final Logger LOG = Logger.getLogger(BinarySourceMapperTestCase.class);
    private volatile AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private long waitTime = 300;
    private long timeout = 2000;

    @BeforeMethod
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void binarySourceMapperTest1() throws InterruptedException, IOException, SubscriberUnAvailableException {
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
                    count.getAndIncrement();
                    switch (count.get()) {
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
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
    }

    @Test(dependsOnMethods = "binarySourceMapperTest1")
    public void binarySourceMapperTest2() throws InterruptedException, IOException, SubscriberUnAvailableException {
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
                    count.getAndIncrement();
                    switch (count.get()) {
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

        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);

        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
    }

    @Test(dependsOnMethods = "binarySourceMapperTest2")
    public void binarySourceMapperTest3() throws InterruptedException, IOException, SubscriberUnAvailableException {
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
                    count.getAndIncrement();
                    switch (count.get()) {
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

        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);

        siddhiAppRuntime.shutdown();

        AssertJUnit.assertFalse(eventArrived);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void binarySourceMapperTest4() throws InterruptedException, IOException {
        LOG.info("binary SourceMapper TestCase for custom mapping");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@source(type='inMemory', topic='WSO2', @map(type='binary', @attributes(a = 'a',b = 'b',c = 'c',"
                + "d = 'd',e = 'e',f = 'f')))\n" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "" +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void binarySourceMapperTest5() throws InterruptedException, IOException, SubscriberUnAvailableException {
        LOG.info("binary SourceMapper TestCase for object type");
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
                    count.getAndIncrement();
                    switch (count.get()) {
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
        InMemoryBroker.publish("WSO2", BinaryEventConverter.convertToBinaryMessage(
                arrayList.toArray(new Event[3]), types).array());
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
    }
}
