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

package org.apache.flume.channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryChannel {

  private Channel channel;

  @Before
  public void setUp() {
    channel = new MemoryChannel();
  }

  @Test
  public void testPutTake() throws InterruptedException, EventDeliveryException {
    Event event = EventBuilder.withBody("test event".getBytes());
    Context context = new Context();

    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    channel.put(event);
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    Event event2 = channel.take();
    Assert.assertEquals(event, event2);
    transaction.commit();
  }

  @Test
  public void testChannelResize() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "5");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    for(int i=0; i < 5; i++) {
      channel.put(EventBuilder.withBody(String.format("test event %d", i).getBytes()));
    }
    transaction.commit();
    transaction.close();

    /*
     * Verify overflow semantics
     */
    transaction = channel.getTransaction();
    boolean overflowed = false;
    try {
      transaction.begin();
      channel.put(EventBuilder.withBody("overflow event".getBytes()));
      transaction.commit();
    } catch (ChannelException e) {
      overflowed = true;
      transaction.rollback();
    } finally {
      transaction.close();
    }
    Assert.assertTrue(overflowed);

    /*
     * Reconfigure capacity down and add another event, shouldn't result in exception
     */
    parms.put("capacity", "6");
    context.putAll(parms);
    Configurables.configure(channel, context);
    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("extended capacity event".getBytes()));
    transaction.commit();
    transaction.close();

    /*
     * Attempt to reconfigure capacity to below current entry count and verify
     * it wasn't carried out
     */
    parms.put("capacity", "2");
    parms.put("transactionCapacity", "2");
    context.putAll(parms);
    Configurables.configure(channel, context);
    for(int i=0; i < 6; i++) {
      transaction = channel.getTransaction();
      transaction.begin();
      Assert.assertNotNull(channel.take());
      transaction.commit();
      transaction.close();
    }
  }

  @Test(expected=ChannelException.class)
  public void testTransactionPutCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "2");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    // shouldn't be able to fit a third in the buffer
    channel.put(EventBuilder.withBody("test".getBytes()));
    Assert.fail();
  }

  @Test(expected=ChannelException.class)
  public void testCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    // this should kill  it
    transaction.commit();
    Assert.fail();
  }

  @Test
  public void testBufferEmptyingAfterTakeCommit() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "3");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    channel.take();
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
  }

  @Test
  public void testBufferEmptyingAfterRollback() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "3");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.rollback();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
  }

  @Test
  public void testTxnListenerTakes() throws IOException{
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "1000");
    parms.put("transactionCapacity", "1000");
    parms.put("listenerfactories", "a b");
    parms.put("listenerfactories.a.type",
        "org.apache.flume.channel.DummyTransactionListenerFactory");
    parms.put("listenerfactories.a.headers", "test");
    parms.put("listenerfactories.b.type",
        "org.apache.flume.channel.DummyTransactionListenerFactory");
    parms.put("listenerfactories.b.headers", "test3");
    context.putAll(parms);
    runTxnListenerTest(context);

    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get("a-" +
            DummyTransactionListener.TOOK), new Long(2));

    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get("b-"+
            DummyTransactionListener.TOOK), new Long(2));

    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get("a-"+
            DummyTransactionListener.COMMITTED), new Long(2));
    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get("b-"+
            DummyTransactionListener.COMMITTED), new Long(1));

  }

  //@Test
  public void testTxnListenerPuts() throws IOException{
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "1000");
    parms.put("transactionCapacity", "1000");
    parms.put("txnlistener.type",
        "org.apache.flume.channel.DummyTransactionListenerFactory");
    parms.put("txnlistener.callon" , "put");
    parms.put("listenerheaders", "test test3");
    context.putAll(parms);
    runTxnListenerTest(context);
    System.out.println(DummyTransactionListener.counterGroup);
    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get(
            DummyTransactionListener.COMMITTED), new Long(2));

    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get(
            DummyTransactionListener.PUT), new Long(3));

    Assert.assertEquals(
        DummyTransactionListener.counterGroup.get(
            DummyTransactionListener.TOOK), new Long(0));
  }

  private void runTxnListenerTest(Context context){
    DummyTransactionListener.resetCounters();
    Configurables.configure(channel,  context);
    Map<String, String> headers;
    Transaction tx = channel.getTransaction();
    tx.begin();
    headers = new HashMap<String, String>();  //valid hdr = 0;
    headers.put("test2", "blah");
    channel.put(EventBuilder.withBody(" ".getBytes(),headers));
    headers = new HashMap<String, String>();
    headers.put("test3", "blah");           //valid hdr = 1
    channel.put(EventBuilder.withBody(" ".getBytes(),headers));
    headers = new HashMap<String, String>();
    headers.put("test", "blah");            //valid hdr = 2
    headers.put("test3", "blah");
    channel.put(EventBuilder.withBody(" ".getBytes(),headers));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    channel.take();
    channel.take();
    tx.commit();
    tx.close();  //2 valid hdrs in this txn, so took =2, commit = 1

    tx = channel.getTransaction();
    tx.begin();
    headers = new HashMap<String, String>();
    headers.put("test4", "blah");
    channel.put(EventBuilder.withBody(" ".getBytes(),headers));
    headers = new HashMap<String, String>();
    headers.put("test", "blah");            //valid hdr = 3
    channel.put(EventBuilder.withBody(" ".getBytes(),headers));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    tx.commit();
    tx.close();   //no valid hdrs

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    tx.commit();
    tx.close();       //1 valid hdr so now took =3, commit =2

    System.out.println("Committed: " + DummyTransactionListener.
        counterGroup.get(DummyTransactionListener.COMMITTED));
    System.out.println("Put: " + DummyTransactionListener.
        counterGroup.get(DummyTransactionListener.PUT));
    System.out.println("Took: " + DummyTransactionListener.
        counterGroup.get(DummyTransactionListener.TOOK));

  }


}
