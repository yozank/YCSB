package com.yahoo.ycsb.db;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * EntryProcessor for update operation.
 */
public class UpdateEP extends AbstractEntryProcessor<String, Map<String, String>>  implements DataSerializable {

  private Map<String, String> values;

  public UpdateEP() {
  }

  public UpdateEP(Map<String, String> values) {
    this.values = values;
  }

  @Override
  public Object process(Map.Entry<String, Map<String, String>> entry) {
    Map<String, String> oldValue = entry.getValue();
    if(oldValue != null) {
      oldValue.putAll(values);
    } else {
      oldValue = values;
    }
    entry.setValue(oldValue);
    return null;
  }

  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
    out.writeInt(values.size());
    for (Map.Entry<String, String> entry: values.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
  }

  @Override
  public void readData(ObjectDataInput in) throws IOException {
    int valCount = in.readInt();
    values = new HashMap<String, String>(valCount);
    for (int i = 0; i < valCount; i++) {
      values.put(in.readUTF(), in.readUTF());
    }
  }
}
