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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.TransactionListenerFactory;
import org.apache.flume.conf.ConfigurationException;

import com.google.common.base.Preconditions;

/**
 * <p>
 * An implementation of basic {@link Channel} semantics, including the
 * implied thread-local semantics of the {@link Transaction} class,
 * which is required to extend {@link BasicTransactionSemantics}.
 * </p>
 * Each channel can specify, in the configuration any number of
 * transaction listener factories. Each transaction listener factory instance
 * is associated with a name, and its properties and headers are defined
 * by referencing the name(see below for format and example).
 * A new transaction listener is created
 * from these factory instances for a transaction if an event with headers
 * matching
 * the header for that factory passes the channel boundary. Each factory can
 * be associated with more than one header, and a transaction listener created
 * by that factory
 * will be called(onPut/take and on commit) exactly once if one or
 * more of the event headers match the
 * headers specified for the factory in the configuration. Every factory instance will create exactly one
 * instance of the listener, which will be called exactly once with the full
 * headers from the event. If a different
 * listener instance is to be called for each of the headers for the same
 * factory class,
 * a different instance of the factory should be configured with
 * each of the headers. If a set of headers are configured with
 * one factory instance, then only one call is made to the listener
 * created by the factory (see example). Sets of headers defined for
 * different factory instances can overlap. A transaction
 * listener factory also accepts a parameter, "callon" which can have one
 * of the two values: <tt>put</tt> or <tt>take</tt>.
 * If the value is not one of these two or
 * is not defined, then it defaults to <tt>take</tt>. If <tt>put</tt>
 * is specified, an instance of this factory is called if an event matching at
 * least one of the factory headers is put into the channel, and if
 * <tt>put</tt> is specified, an instance of this factory is called if an
 * event matching at
 * least one of the factory headers is taken from the channel.
 * <p>If <tt>headers
 * </tt> and <tt>type</tt> are not specified, the listener factory is never
 * used. You can have multiple factory instances configured for the same (set of)
 * headers, and have one factory instance configured for the multiple headers.
 * You can also have multiple factory instances configured to use the
 * same factory class. This will cause the creation of one listener per
 * factory instance, per transaction, this way you can have different
 * sets of headers call different instances of the same listener class.
 * This accepts configuration parameters in the following way: <p>
 *
 * <pre>
 * agent.channels.channel.listenerfactories = a b c
 * agent.channels.channel.listenerfactories.a.type = fully.qualified.classname
 * agent.channels.channel.listenerfactories.a.headers = space separated list of headers
 * agent.channels.channel.listenerfactories.a.callon = put/take
 * </pre><p>
 * Example: <p>
 * <pre>
 * agent.channels.channel.listenerfactories = a b c
 * agent.channels.channel.listenerfactories.a.type  = org.apache.channel.memory.DummyTransactionListener
 * agent.channels.channel.listenerfactories.a.callon = put
 * agent.channels.channel.listenerfactories.a.headers = test1 test5
 * agent.channels.channel.listenerfactories.b.type  = org.apache.channel.memory.SmartTransactionListener
 * agent.channels.channel.listenerfactories.b.callon = take
 * agent.channels.channel.listenerfactories.b.headers = test3 test6 test21
 *
 * agent.channels.channel.listenerfactories.c.type  = org.apache.channel.memory.DummyTransactionListener
 * agent.channels.channel.listenerfactories.c.callon = take
 * agent.channels.channel.listenerfactories.c.headers = test3 test6 test21
 * <pre> <p>
 *
 * <strong>Note: </strong> In the above description, calling a transaction
 * listener means that its onPut/onTake methods are called on put/take and
 * its onTransactionCommit/onTransactionRollback are called when the
 * transaction is committed or rolled back </p>
 * </p>
 */

public abstract class BasicChannelSemantics extends AbstractChannel {

  private ThreadLocal<BasicTransactionSemantics> currentTransaction
      = new ThreadLocal<BasicTransactionSemantics>();

  private Map<String, Set<TransactionListenerFactory>> headerFactoryMap =
      new HashMap<String, Set<TransactionListenerFactory>>();

  private Set<TransactionListenerFactory> callOnPut =
      new HashSet<TransactionListenerFactory>();
  private Set<TransactionListenerFactory> callOnTake =
      new HashSet<TransactionListenerFactory>();


  private boolean initialized = false;


  /**
   * <p>
   * Called upon first getTransaction() request, while synchronized on
   * this {@link Channel} instance.  Use this method to delay the
   * initialization resources until just before the first
   * transaction begins. <p>
   */
  protected void initialize() {}

  /**
   * <p>
   * Called to create new {@link Transaction} objects, which must
   * extend {@link BasicTransactionSemantics}.  Each object is used
   * for only one transaction, but is stored in a thread-local and
   * retrieved by <code>getTransaction</code> for the duration of that
   * transaction.
   * </p>
   */
  protected abstract BasicTransactionSemantics createTransaction();

  /**
   * <p>
   * Ensures that a transaction exists for this thread and then
   * delegates the <code>put</code> to the thread's {@link
   * BasicTransactionSemantics} instance.
   * </p>
   */
  @Override
  public void put(Event event) throws ChannelException {
    BasicTransactionSemantics transaction = currentTransaction.get();
    Preconditions.checkState(transaction != null,
        "No transaction exists for this thread");
    transaction.put(event);
  }

  /**
   * <p>
   * Ensures that a transaction exists for this thread and then
   * delegates the <code>take</code> to the thread's {@link
   * BasicTransactionSemantics} instance.
   * </p>
   */
  @Override
  public Event take() throws ChannelException {
    BasicTransactionSemantics transaction = currentTransaction.get();
    Preconditions.checkState(transaction != null,
        "No transaction exists for this thread");
    return transaction.take();
  }

  /**
   * <p>
   * Initializes the channel if it is not already, then checks to see
   * if there is an open transaction for this thread, creating a new
   * one via <code>createTransaction</code> if not.
   * @return the current <code>Transaction</code> object for the
   *     calling thread
   * </p>
   */
  @Override
  public Transaction getTransaction() {

    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          initialize();
          initialized = true;
        }
      }
    }

    BasicTransactionSemantics transaction = currentTransaction.get();
    if (transaction == null || transaction.getState().equals(
        BasicTransactionSemantics.State.CLOSED)) {
      transaction = createTransaction();
      if(!headerFactoryMap.isEmpty()){
        transaction.setHasTxnListener();
        transaction.setFactoriesToCallOnPut(callOnPut);
        transaction.setFactoriesToCallOnTake(callOnTake);
        transaction.setHeaderFactoryMap(headerFactoryMap);
      }
      currentTransaction.set(transaction);
    }
    return transaction;
  }

  /**
   * Accepts basic configuration for the transaction listener factories
   * and other
   * channel specific properties.
   * <bold> All subclasses must call <tt>super.configure()</tt> in their
   * configure methods, for transaction listener support. If this method is not
   * called transaction listeners will not be instantiated or configured.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Context context) {
    try {
    int i = 0;
    String factoryNames = context.getString("listenerfactories");
    if(factoryNames != null && !factoryNames.isEmpty()){
      String[] factories = factoryNames.split("\\s+");
      for(String currentFactory:factories){
        Map<String, String> currentFactoryProps = context.getSubProperties(
            "listenerfactories"+"."+currentFactory+".");
        String factoryType = currentFactoryProps.get("type").trim();
        Context currentFactoryContext = new Context();
        currentFactoryContext.putAll(currentFactoryProps);
        TransactionListenerFactory instance = null;
        if(factoryType != null && !factoryType.isEmpty()){
          try {
            Class<? extends TransactionListenerFactory> factory =
                (Class<? extends TransactionListenerFactory>)
                Class.forName(factoryType);
            instance = factory.newInstance();
            instance.setName(currentFactory);
            instance.initialize(currentFactoryContext);
          } catch (Exception e) {
            throw new ConfigurationException("Could not create Transaction" +
                "Listener Factory. Exception follows.", e);
          }
          String currentHeaders = currentFactoryContext.getString("headers");
          if(currentHeaders != null && !currentHeaders.isEmpty()){
            String[] headers = currentHeaders.split("\\s+");
            for(String header:headers){
              Set<TransactionListenerFactory> factoriesToCall = null;
              if(this.headerFactoryMap.containsKey(header)){
                factoriesToCall = headerFactoryMap.get(header);
              } else {
                factoriesToCall = new HashSet<TransactionListenerFactory>();
                headerFactoryMap.put(header, factoriesToCall);
              }
              factoriesToCall.add(instance);
            }
          }
        }
        String when = currentFactoryContext.getString("callon");
        if(when != null && when.isEmpty()) {
          if(when.equals("put")){
            callOnPut.add(instance);
          } else {
            callOnTake.add(instance);
          }
        } else {
          callOnTake.add(instance);
        }
      }
    }
    } catch (Exception e) {
      throw new ConfigurationException(
          "Error configuring channel: " + getName(), e);

    }

  }
}