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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.*;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.*;

import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaChannel extends BasicChannelSemantics {

  private final static Logger LOGGER =
    LoggerFactory.getLogger(KafkaChannel.class);


  private final Properties kafkaConf = new Properties();
  private Producer<String, byte[]> producer;

  private AtomicReference<String> topic = new AtomicReference<String>();
  private final Map<String, Integer> topicCountMap =
    Collections.synchronizedMap(new HashMap<String, Integer>());

  // Track all consumers to close them eventually.
  private final List<ConsumerAndIterator> consumers =
    Collections.synchronizedList(new LinkedList<ConsumerAndIterator>());
  private final ThreadLocal<List<Event>> failedEvents = new
    ThreadLocal<List<Event>>() {
      @Override
      protected List<Event> initialValue() {
        return new LinkedList<Event>();
      }
    };

  /* Each ConsumerConnector commit will commit all partitions owned by it. To
   * ensure that each partition is only committed when all events are
   * actually done, we will need to keep a ConsumerConnector per thread.
   * See Neha's answer here:
   * http://grokbase.com/t/kafka/users/13b4gmk2jk/commit-offset-per-topic
   * Since only one consumer connector will a partition at any point in time,
   * when we commit the partition we would have committed all events to the
   * final destination from that partition.
   *
   * If a new partition gets assigned to this connector,
   * my understanding is that all message from the last partition commit will
   * get replayed which may cause duplicates -- which is fine as this
   * happens only on partition rebalancing which is on failure or new nodes
   * coming up, which is rare.
   */
  private final ThreadLocal<ConsumerAndIterator> consumerAndIter = new
    ThreadLocal<ConsumerAndIterator>() {

      @Override
      public ConsumerAndIterator initialValue() {
        try {
          ConsumerConfig consumerConfig = new ConsumerConfig(kafkaConf);
          ConsumerConnector consumer =
            Consumer.createJavaConsumerConnector(consumerConfig);
          Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumer.createMessageStreams(topicCountMap);
          final List<KafkaStream<byte[], byte[]>> streamList = consumerMap
            .get(topic.get());
          ConsumerAndIterator ret = new ConsumerAndIterator();
          synchronized (kafkaConf) {
            KafkaStream<byte[], byte[]> stream = streamList.remove(0);
            ret.consumer = consumer;
            ret.iterator = stream.iterator();
            consumers.add(ret);
            LOGGER.info("Created new consumer to connect to Kafka");
          }
          return ret;
        } catch (Exception e) {
          throw new FlumeException("Unable to connect to Kafka", e);
        }
      }
    };

  @Override
  public void start() {
    try {
      LOGGER.info("Starting Kafka Channel: " + getName());
      synchronized (kafkaConf) {
        producer = new Producer<String, byte[]>(new ProducerConfig(kafkaConf));
      }
      // We always have just one topic being read by one thread
      LOGGER.info("Topic = " + topic.get());
      topicCountMap.put(topic.get(), 1);
      super.start();
    } catch (Exception e) {
      LOGGER.error("Could not start producer");
      throw new FlumeException("Unable to create Kafka Connections. " +
        "Check whether the ZooKeeper server is up and that the " +
        "Flume agent can connect to it.", e);
    }
  }

  @Override
  public void stop() {
    for (ConsumerAndIterator c : consumers) {
      try {
        c.consumer.shutdown();
      } catch (Exception ex) {
        LOGGER.warn("Error while shutting down consumer.", ex);
      }
    }
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new KafkaTransaction();
  }

  @Override
  public void configure(Context ctx) {
    String topicStr = ctx.getString(TOPIC);
    if (topicStr == null || topicStr.isEmpty()) {
      topicStr = DEFAULT_TOPIC;
      LOGGER
        .info("Topic was not specified. Using " + topicStr + " as the topic.");
    }
    topic.set(topicStr);
    String groupId = ctx.getString(GROUP_ID_FLUME);
    if (groupId == null || groupId.isEmpty()) {
      groupId = DEFAULT_GROUP_ID;
      LOGGER.info(
        "Group ID was not specified. Using " + groupId + " as the group id.");
    }
    String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
    if (brokerList == null || brokerList.isEmpty()) {
      throw new ConfigurationException("Broker List must be specified");
    }
    String zkConnect = ctx.getString(ZOOKEEPER_CONNECT_FLUME_KEY);
    if (zkConnect == null || zkConnect.isEmpty()) {
      throw new ConfigurationException(
        "Zookeeper Connection must be specified");
    }
    Long timeout = ctx.getLong(TIMEOUT, Long.valueOf(DEFAULT_TIMEOUT));
    synchronized (kafkaConf) {
      kafkaConf.putAll(ctx.getSubProperties(KAFKA_PREFIX));
      kafkaConf.put(GROUP_ID, groupId);
      kafkaConf.put(BROKER_LIST_KEY, brokerList);
      kafkaConf.put(ZOOKEEPER_CONNECT, zkConnect);
      kafkaConf.put(AUTO_COMMIT_ENABLED, String.valueOf(false));
      kafkaConf.put(CONSUMER_TIMEOUT, String.valueOf(timeout * 100));
      kafkaConf.put(REQUIRED_ACKS_KEY, "-1");
//    kafkaConf.put(MESSAGE_SERIALIZER_KEY, MESSAGE_SERIALIZER);
//    kafkaConf.put(KEY_SERIALIZER_KEY, KEY_SERIALIZER);
      kafkaConf.put("producer.type", "sync");
      LOGGER.info(kafkaConf.toString());
    }
  }

  private enum TransactionType {
    PUT,
    TAKE
  }

  private class KafkaTransaction extends BasicTransactionSemantics {

    private TransactionType type;
    // For Puts
    private Optional<ByteArrayOutputStream> tempOutStream = Optional
      .absent();
    private Optional<ObjectOutputStream> tempDataOut = Optional.absent();

    // For put transactions, serialize the events and batch them and send it.
    private List<byte[]> serializedEvents = null;
    // For take transactions, deserialize and hold them till commit goes through
    private List<Event> events = null;
    private boolean removed = false;

    @Override
    protected void doPut(Event event) throws InterruptedException {
      type = TransactionType.PUT;
      if (serializedEvents == null) {
        serializedEvents = Lists.newLinkedList();
      }
      try {
        if (!tempDataOut.isPresent()) {
          tempOutStream = Optional.of(new ByteArrayOutputStream());
        }
        tempOutStream.get().reset();
        AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()
        ), ByteBuffer.wrap(event.getBody()));
        Encoder encoder =
          EncoderFactory.get().directBinaryEncoder(tempOutStream.get(), null);
        SpecificDatumWriter<AvroFlumeEvent> writer = new SpecificDatumWriter
          <AvroFlumeEvent>(AvroFlumeEvent.class);
        writer.write(e, encoder);
        serializedEvents.add(tempOutStream.get().toByteArray());
        // Not really possible to avoid this copy :(
      } catch (Exception e) {
        e.printStackTrace();
        throw new ChannelException("Error while serializing event", e);
      }
    }

    @Override
    protected Event doTake() throws InterruptedException {
      type = TransactionType.TAKE;
      if (events == null) {
        events = Lists.newLinkedList();
      }
      Event e;
      if (!failedEvents.get().isEmpty()) {
        e = failedEvents.get().remove(0);
        removed = true;
      } else {
        try {
          ConsumerIterator<byte[], byte[]> it = consumerAndIter.get().iterator;
          it.hasNext();
          ByteArrayInputStream in = new ByteArrayInputStream(
            it.next().message());
          Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
          SpecificDatumReader<AvroFlumeEvent> reader = new
            SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class);
          AvroFlumeEvent event = reader.read(null, decoder);
          e = EventBuilder.withBody(event.getBody().array(),
            toStringMap(event.getHeaders()));
          removed = true;
        } catch (ConsumerTimeoutException ex) {
          LOGGER.warn("Error - timeout", ex);
          return null;
        } catch (Exception ex) {
          LOGGER.warn("Error", ex);
          throw new ChannelException("Error while getting events from Kafka",
            ex);
        }
      }
      LOGGER.info("Got event from kafka!!");
      events.add(e);
      return e;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (type.equals(TransactionType.PUT)) {
        try {
          List<KeyedMessage<String, byte[]>> messages = new
            ArrayList<KeyedMessage<String, byte[]>>(serializedEvents.size());
          for (byte[] event : serializedEvents) {
            messages.add(new KeyedMessage<String, byte[]>(topic.get(), event));
          }
          producer.send(messages);
          serializedEvents.clear();
          LOGGER.info("Put events");
        } catch (Exception ex) {
          LOGGER.warn("Sending events to Kafka failed", ex);
          throw new ChannelException("Commit failed as send to Kafka failed",
            ex);
        }
      } else {
        if (failedEvents.get().isEmpty() && removed) {
          consumerAndIter.get().consumer.commitOffsets();
        }
        events.clear();
      }
    }

    @Override
    protected void doRollback() throws InterruptedException {
      if (type.equals(TransactionType.PUT)) {
        serializedEvents.clear();
      } else {
        failedEvents.get().addAll(events);
        events.clear();
      }
    }
  }


  private class ConsumerAndIterator {
    ConsumerConnector consumer;
    ConsumerIterator<byte[], byte[]> iterator;
  }
  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(
    Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
      new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(
    Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap =
      new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }
}

class KafkaChannelEvent implements Serializable {

  private static final long serialVersionUID = 8702461677509231736L;

  final Map<String, String> headers;
  final byte[] body;

  KafkaChannelEvent(Map<String, String> headers, byte[] body) {
    this.headers = headers;
    this.body = body;
  }
}