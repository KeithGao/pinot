/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code DataTypeTransformer} class will convert the values to follow the data types in {@link FieldSpec}.
 */
public class DataTypeTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTypeTransformer.class);

  private static final Map<Class, PinotDataType> SINGLE_VALUE_TYPE_MAP = new HashMap<>();
  private static final Map<Class, PinotDataType> MULTI_VALUE_TYPE_MAP = new HashMap<>();

  static {
    SINGLE_VALUE_TYPE_MAP.put(Boolean.class, PinotDataType.BOOLEAN);
    SINGLE_VALUE_TYPE_MAP.put(Byte.class, PinotDataType.BYTE);
    SINGLE_VALUE_TYPE_MAP.put(Character.class, PinotDataType.CHARACTER);
    SINGLE_VALUE_TYPE_MAP.put(Short.class, PinotDataType.SHORT);
    SINGLE_VALUE_TYPE_MAP.put(Integer.class, PinotDataType.INTEGER);
    SINGLE_VALUE_TYPE_MAP.put(Long.class, PinotDataType.LONG);
    SINGLE_VALUE_TYPE_MAP.put(Float.class, PinotDataType.FLOAT);
    SINGLE_VALUE_TYPE_MAP.put(Double.class, PinotDataType.DOUBLE);
    SINGLE_VALUE_TYPE_MAP.put(String.class, PinotDataType.STRING);
    SINGLE_VALUE_TYPE_MAP.put(byte[].class, PinotDataType.BYTES);
    SINGLE_VALUE_TYPE_MAP.put(ByteArray.class, PinotDataType.BYTES);

    MULTI_VALUE_TYPE_MAP.put(Byte.class, PinotDataType.BYTE_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Character.class, PinotDataType.CHARACTER_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Short.class, PinotDataType.SHORT_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Integer.class, PinotDataType.INTEGER_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Long.class, PinotDataType.LONG_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Float.class, PinotDataType.FLOAT_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Double.class, PinotDataType.DOUBLE_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(String.class, PinotDataType.STRING_ARRAY);
  }

  private final Map<String, PinotDataType> _dataTypes = new HashMap<>();

  public DataTypeTransformer(Schema schema) {
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      _dataTypes.put(entry.getKey(), PinotDataType.getPinotDataType(entry.getValue()));
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, PinotDataType> entry : _dataTypes.entrySet()) {
      String column = entry.getKey();
      PinotDataType dest = entry.getValue();
      Object value = record.getValue(column);

      // NOTE: should not be null in normal case if combined with other transformers (without TimeTransformer, outgoing
      // time column might be null)
      if (value == null) {
        continue;
      }

      PinotDataType source;
      if (value instanceof Object[]) {
        // Multi-valued column
        Object[] values = (Object[]) value;
        assert values.length > 0;
        source = MULTI_VALUE_TYPE_MAP.get(values[0].getClass());
        if (source == null) {
          source = PinotDataType.OBJECT_ARRAY;
        }
      } else {
        // Single-valued column
        source = SINGLE_VALUE_TYPE_MAP.get(value.getClass());
        if (source == null) {
          source = PinotDataType.OBJECT;
        }
      }

      if (source != dest) {
        value = dest.convert(value, source);
      }

      // Null character is used as the padding character, so we do not allow null characters in strings.
      if (dest == PinotDataType.STRING) {
        if (StringUtil.containsNullCharacter(value.toString())) {
          LOGGER.error("Input value: {} for column: {} contains null character", value, column);
          value = StringUtil.removeNullCharacters(value.toString());
        }
      }

      // Wrap primitive byte[] into Bytes, this is required as the value read has to be Comparable,
      // as well as have equals() and hashCode() methods so it can be a key in a Map/Set.
      if (dest == PinotDataType.BYTES && value instanceof byte[]) {
        value = new ByteArray((byte[]) value);
      }

      record.putField(column, value);
    }
    return record;
  }
}
