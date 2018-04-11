/**
 * Copyright (c) 2012-2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;


import java.util.*;

/**
 * This is a client implementation for Hazelcast 3.x.
 */
public class HazelcastDBClient extends DB {

  private HazelcastInstance hz;

  public void init() throws DBException {
    try {
      ClientConfig config = new XmlClientConfigBuilder().build();
//      config.getUserCodeDeploymentConfig().addClass(UpdateEP.class).setEnabled(true);
      hz = HazelcastClient.newHazelcastClient(config);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  public void cleanup() {
    hz.shutdown();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, String> data = (Map<String, String>) hz.getMap(table).get(key);
    if (fields == null || fields.isEmpty()) {
      StringByteIterator.putAllAsByteIterators(result, data);
    } else {
      for (String field : fields) {
        result.put(field, new StringByteIterator(data.get(field)));
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    Predicate predicate = Predicates.and(Predicates.greaterEqual("__key", startkey), Predicates.lessThan("__key", Integer.parseInt(startkey) + recordcount));
    IMap<String, Map<String, String> > map = hz.getMap(table);
    Set<Map.Entry<String, Map<String, String>>> entries = map.entrySet(predicate);
    if(fields == null || fields.isEmpty()) {
      for(Map.Entry<String, Map<String, String>> entry : entries) {
        result.add((HashMap<String, ByteIterator>)StringByteIterator.getByteIteratorMap( entry.getValue()));
      }
    } else {
       //
    }


    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    hz.getMap(table).executeOnKey(key, new UpdateEP(StringByteIterator.getStringMap(values)));
//    hz.getMap(table).set(key,StringByteIterator.getStringMap(values));
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    hz.getMap(table).set(key, StringByteIterator.getStringMap(values));
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    hz.getMap(table).delete(key);
    return Status.OK;
  }

}
