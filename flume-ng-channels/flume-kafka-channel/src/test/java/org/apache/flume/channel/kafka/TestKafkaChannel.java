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

import com.google.common.collect.Lists;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestKafkaChannel {

  private static TestUtil testUtil = TestUtil.getInstance();

  @Before
  public void setup() throws Exception{
    testUtil.prepare();
    try {
      createTopic(KafkaChannelConfiguration.DEFAULT_TOPIC);
    } catch (Exception e) {
    }
    Thread.sleep(2000);
  }

  @After
  public void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testSuccess() throws Exception {
    Context context = prepareDefaultContext();
    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    channel.start();
    writeAndVerify(false, channel);
    channel.stop();
  }

  @Test
  public void testRollbacks() throws Exception {
    Context context = prepareDefaultContext();
    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    channel.start();
    writeAndVerify(true, channel);
    channel.stop();
  }

  @Test
  public void testStopAndStart() throws Exception {
    doTestStopAndStart(false);
  }

  @Test
  public void testStopAndStartWithRollback() throws Exception {
    doTestStopAndStart(true);
  }

  private void doTestStopAndStart(boolean rollback) throws Exception {
    final KafkaChannel channel = startChannel();
    ExecutorCompletionService<Void> submitterSvc =
      new ExecutorCompletionService<Void>(Executors
        .newCachedThreadPool());
    final List<List<Event>> events = createBaseList();
    putEvents(channel, events, submitterSvc);
    int completed = 0;
    while (completed < 5) {
      submitterSvc.take();
      completed++;
    }
    channel.stop();

    final KafkaChannel channel2 = startChannel();
    final List<Event> eventsPulled =
      pullEvents(channel2, submitterSvc, rollback);
    waitAndVerify(eventsPulled, submitterSvc, 5);
  }

  private KafkaChannel startChannel() {
    Context context = prepareDefaultContext();
    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    channel.start();
    return channel;
  }
  private void writeAndVerify(final boolean testRollbacks,
    final KafkaChannel channel) throws Exception{

    final List<List<Event>> events = createBaseList();

    ExecutorCompletionService<Void> submitterSvc =
      new ExecutorCompletionService<Void>(Executors
        .newCachedThreadPool());

    final List<Event> eventsPulled =
      pullEvents(channel, submitterSvc, testRollbacks);

    Thread.sleep(5000);

    putEvents(channel, events, submitterSvc);

    waitAndVerify(eventsPulled, submitterSvc, 10);
  }

  private List<List<Event>> createBaseList() {
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
    return events;
  }

  private void putEvents(final KafkaChannel channel, final List<List<Event>>
    events, ExecutorCompletionService<Void> submitterSvc){
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
  }

  private List<Event> pullEvents(final KafkaChannel channel,
    ExecutorCompletionService<Void> submitterSvc, final boolean testRollbacks) {
    final List<Event> eventsPulled = Collections.synchronizedList(new
      ArrayList<Event>(50));
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicBoolean rolledBack = new AtomicBoolean(false);
    for (int k = 0; k < 5; k++) {
      final int index = k;
      submitterSvc.submit(new Callable<Void>() {
        @Override
        public Void call() {
          final AtomicBoolean startedGettingEvents = new AtomicBoolean(false);
          Transaction tx = null;
          final List<Event> eventsLocal = Lists.newLinkedList();
          while (counter.get() < 50) {
            if (tx == null) {
              tx = channel.getTransaction();
              tx.begin();
            }
            try {
              Event e = channel.take();
              if (e != null) {
                startedGettingEvents.set(true);
                eventsLocal.add(e);
              } else {
                if (testRollbacks && index == 1 && !rolledBack.get() &&
                  startedGettingEvents.get()) {
                  tx.rollback();
                  System.out.println("Rolledback");
                  rolledBack.set(true);
                  eventsLocal.clear();
                } else {
                  tx.commit();
                  eventsPulled.addAll(eventsLocal);
                  counter.getAndAdd(eventsLocal.size());
                }
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
    return eventsPulled;
  }

  private void waitAndVerify(final List<Event> eventsPulled,
    ExecutorCompletionService<Void> submitterSvc, int max) throws Exception {
    int completed = 0;
    while (completed < max) {
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
