/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.kafka;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestKafkaChannel {

  private static TestUtil testUtil = TestUtil.getInstance();

  @BeforeClass
  public static void setup() {
    testUtil.prepare();
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testKafkaChannel() {
    Context context = prepareDefaultContext();
    KafkaChannel channel = new KafkaChannel();
    List<List<Event>> events = new ArrayList<List<Event>>();
    for (int i = 0; i < 5; i++) {
      List<Event> eventList = new ArrayList<Event>(10);
      for (int j = 0; j < 10; j++) {
        Map<String, String> hdrs = new HashMap<String, String>();
        hdrs.put("header", String.valueOf(j));
        eventList.add(EventBuilder.withBody(String.valueOf(j).getBytes(),
          hdrs));
      }
    }


  }

  private Context prepareDefaultContext() {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put(KafkaChannelConfiguration.BROKER_LIST_KEY,
      testUtil.getKafkaServerUrl());
    context.put(KafkaChannelConfiguration.ZOOKEEPER_CONNECT_FLUME_KEY,
      testUtil.getZkUrl());
    return context;
  }
}
