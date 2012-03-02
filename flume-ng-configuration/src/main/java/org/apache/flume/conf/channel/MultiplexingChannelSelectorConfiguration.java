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
package org.apache.flume.conf.channel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flume.conf.Context;
import org.apache.flume.conf.AbstractComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;

import com.google.common.collect.ImmutableMap;

public class MultiplexingChannelSelectorConfiguration extends
    AbstractComponentConfiguration {
  public static final String CONFIG_MULTIPLEX_HEADER_NAME = "header";
  public static final String DEFAULT_MULTIPLEX_HEADER = "flume.selector.header";
  public static final String CONFIG_PREFIX_MAPPING = "mapping";
  public static final String CONFIG_PREFIX_DEFAULT = "default";
  public static final String CONFIG_PREFIX_OPTIONAL = "optional";
  private String headerName;
  private List<String> defaultChannelNames;
  private Map<String, List<String>> optionalChannels;
  private Map<String, List<String>> requiredChannels;
  private Set<String> channelNames;

  private MultiplexingChannelSelectorConfiguration(Context context) {
    super(context);
  }

  public MultiplexingChannelSelectorConfiguration(Context context,
      Set<String> channelNames) throws ConfigurationException {
    super(context);
    this.channelNames = channelNames;
    this.configure();
  }

  private Map<String, List<String>> getHeaderChannelMap(
      ImmutableMap<String, String> immutableMap) throws ConfigurationException {

    Map<String, List<String>> channels = new HashMap<String, List<String>>();
    for (String headerValue : immutableMap.keySet()) {
      String val = String.valueOf(immutableMap.get(headerValue));

      if (val == null || val.isEmpty()) {
        throw new ConfigurationException("No required channel configured "
            + "when header value is: " + headerValue);
      }
      List<String> arr =
          Arrays.asList(immutableMap.get(headerValue).toString().split(" "));

      if (!isChannelListValid(arr)) {
        throw new ConfigurationException("Invalid channel List "
            + "specified for header: " + headerName);

      }

      if (channels.put(headerValue, arr) != null) {
        throw new ConfigurationException("Required channels configured more "
            + "than once for header: " + headerName);
      }
    }
    return channels;
  }

  private boolean isChannelListValid(List<String> channels) {
    for (String ch : channels) {
      if (!channelNames.contains(ch))
        return false;
    }
    return true;
  }

  @Override
  public void configure() throws ConfigurationException {
    headerName =
        context.getString(CONFIG_MULTIPLEX_HEADER_NAME,
            DEFAULT_MULTIPLEX_HEADER);
    if (headerName == null || headerName.isEmpty()) {
      throw new ConfigurationException("Empty Header name to multiplex on! ");
    }

    String dataRead = context.getString(CONFIG_PREFIX_DEFAULT);
    if (dataRead == null || dataRead.isEmpty())
      throw new ConfigurationException("No default channels specified! ");

    defaultChannelNames = Arrays.asList(dataRead.split(" "));

    if (!isChannelListValid(defaultChannelNames)) {
      throw new ConfigurationException("Invalid default channel List "
          + "specified for header: " + headerName);
    }

    requiredChannels =
        getHeaderChannelMap(context.getSubProperties(CONFIG_PREFIX_MAPPING
            + "."));

    optionalChannels =
        getHeaderChannelMap(context.getSubProperties(CONFIG_PREFIX_OPTIONAL
            + "."));
  }

}
