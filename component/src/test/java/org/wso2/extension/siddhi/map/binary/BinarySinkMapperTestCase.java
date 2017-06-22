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
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.nio.ByteBuffer;
import java.util.ArrayList;


/**
 * TCP sinkmapper test case.
 */
public class BinarySinkMapperTestCase {
    static final Logger LOG = Logger.getLogger(BinarySinkMapperTestCase.class);
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
    public void binarySinkMapperTest1() throws InterruptedException {
        LOG.info("binary SinkMapper TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "" +
                "@sink(type='inMemory', topic='WSO2', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("outputStream").getAttributeList());

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                Event[] events = SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(((ByteBuffer) o).array
                        ()), types);
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

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);

        siddhiAppRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriber);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
    }

    @Test(dependsOnMethods = "binarySinkMapperTest1")
    public void binarySinkMapperTest2() throws InterruptedException {
        LOG.info("binary SinkMapper TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "" +
                "@sink(type='inMemory', topic='WSO2', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("outputStream").getAttributeList());

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {

            @Override
            public void onMessage(Object o) {
                Event[] events = SiddhiEventConverter.toConvertToSiddhiEvents(
                        ByteBuffer.wrap(((ByteBuffer) o).array()), types);
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList1 = new ArrayList<Event>();
        arrayList1.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36.0, 3.0f, 380L, 23.0, true}));
        arrayList1.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        inputHandler.send(arrayList1.toArray(new Event[2]));

        ArrayList<Event> arrayList2 = new ArrayList<Event>();
        arrayList2.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList2.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList2.toArray(new Event[2]));

        Thread.sleep(3000);

        siddhiAppRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriber);

        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);


    }

}

