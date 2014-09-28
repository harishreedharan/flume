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

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestKafkaChannel {

  private static TestUtil testUtil = TestUtil.getInstance();

  @BeforeClass
  public static void setup() throws Exception{
    testUtil.prepare();
    try {
      createTopic(KafkaChannelConfiguration.DEFAULT_TOPIC);
    } catch (Exception e) {
    }
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testKafkaChannel() throws Exception {
    Context context = prepareDefaultContext();
    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    channel.start();
    final List<List<Event>> events = new ArrayList<List<Event>>();
    for (int i = 0; i < 5; i++) {
      List<Event> eventList = new ArrayList<Event>(10);
      events.add(eventList);
      for (int j = 0; j < 10; j++) {
        Map<String, String> hdrs = new HashMap<String, String>();
        String v = (String.valueOf(i) + " - " + String
          .valueOf(j));
        hdrs.put("header", v);
        eventList.add(EventBuilder.withBody(v.getBytes(), hdrs));
      }
    }
    ExecutorCompletionService<Void> submitterSvc =
      new ExecutorCompletionService<Void>(Executors
        .newCachedThreadPool());
    final List<Event> eventsPulled = Collections.synchronizedList(new
      ArrayList<Event>(50));
    final AtomicInteger counter = new AtomicInteger(0);
    for (int k = 0; k < 5; k++) {
      submitterSvc.submit(new Callable<Void>() {
        @Override
        public Void call() {
          Transaction tx = null;
          while (counter.get() < 50) {
            if (tx == null) {
              tx = channel.getTransaction();
              tx.begin();
            }
            try {
              Event e = channel.take();
              if (e != null) {
                eventsPulled.add(e);
                counter.incrementAndGet();
              } else {
                tx.commit();
                tx.close();
                tx = null;
              }
            } catch (Exception ex) {
              ex.printStackTrace();
            }
          }
          return null;
        }
      });
    }
    Thread.sleep(5000);
    final int totalEvents = 50;
    for (int i = 0; i < 5; i++) {
      final int index = i;
      submitterSvc.submit(new Callable<Void>() {
        @Override
        public Void call() {
          Transaction tx = channel.getTransaction();
          tx.begin();
          List<Event> eventsToPut = events.get(index);
          for (int j = 0; j < 10; j++) {
            channel.put(eventsToPut.get(j));
          }
          try {
            tx.commit();
          } finally {
            tx.close();
          }
          return null;
        }
      });
    }
    int completed = 0;
      while (completed < 10) {
        submitterSvc.take();
        completed++;
      }
    Assert.assertFalse(eventsPulled.isEmpty());
    Assert.assertTrue(eventsPulled.size() == 50);
    Set<String> eventStrings = new HashSet<String>();
    for(Event e : eventsPulled) {
      Assert.assertEquals(e.getHeaders().get("header"), new String(e.getBody()));
      eventStrings.add(e.getHeaders().get("header"));
    }
    for(int i = 0; i < 5; i++) {
      for (int j = 0; j < 10; j++) {
        String v = String.valueOf(i) + " - " + String.valueOf(j);
        Assert.assertTrue(eventStrings.contains(v));
        eventStrings.remove(v);
      }
    }
    Assert.assertTrue(eventStrings.isEmpty());
  }

  private Context prepareDefaultContext() {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put(KafkaChannelConfiguration.BROKER_LIST_FLUME_KEY,
      testUtil.getKafkaServerUrl());
    context.put(KafkaChannelConfiguration.ZOOKEEPER_CONNECT_FLUME_KEY,
      testUtil.getZkUrl());
    return context;
  }

  public static void createTopic(String topicName) {
    // Create a ZooKeeper client
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(testUtil.getZkUrl(),
      sessionTimeoutMs, connectionTimeoutMs,
      ZKStringSerializer$.MODULE$);

    int numPartitions = 5;
    int replicationFactor = 1;
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkClient, topicName, numPartitions,
      replicationFactor, topicConfig);
  }
}
