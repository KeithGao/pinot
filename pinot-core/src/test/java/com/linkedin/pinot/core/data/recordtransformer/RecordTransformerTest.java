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
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class RecordTransformerTest {
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.INT)
          .addSingleValueDimension("svLong", FieldSpec.DataType.LONG)
          .addSingleValueDimension("svFloat", FieldSpec.DataType.FLOAT)
          .addSingleValueDimension("svDouble", FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
          .addSingleValueDimension("svBytes", FieldSpec.DataType.BYTES)
          .addMultiValueDimension("mvInt", FieldSpec.DataType.INT)
          .addMultiValueDimension("mvLong", FieldSpec.DataType.LONG)
          .addMultiValueDimension("mvFloat", FieldSpec.DataType.FLOAT)
          .addMultiValueDimension("mvDouble", FieldSpec.DataType.DOUBLE)
          .addMultiValueDimension("mvString", FieldSpec.DataType.STRING)
          .addTime("incoming", 6, TimeUnit.HOURS, FieldSpec.DataType.INT, "outgoing", 1, TimeUnit.MILLISECONDS,
              FieldSpec.DataType.LONG)
          .build();
  // Transform multiple times should return the same result
  private static final int NUM_ROUNDS = 5;

  private static GenericRow getRecord() {
    GenericRow record = new GenericRow();
    Map<String, Object> fields = new HashMap<>();
    fields.put("svInt", (byte) 123);
    fields.put("svLong", (char) 123);
    fields.put("svFloat", (short) 123);
    fields.put("svDouble", "123");
    fields.put("svString", new Object() {
      @Override
      public String toString() {
        return "123";
      }
    });
    fields.put("svBytes", new byte[]{123});
    fields.put("mvInt", new Object[]{123L});
    fields.put("mvLong", new Object[]{123f});
    fields.put("mvFloat", new Object[]{123d});
    fields.put("mvDouble", new Object[]{123});
    fields.put("mvString", true);
    fields.put("incoming", "123");
    record.init(fields);
    return record;
  }

  @Test
  public void testDataTypeTransformer() {
    DataTypeTransformer transformer = new DataTypeTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), 123);
      assertEquals(record.getValue("svLong"), 123L);
      assertEquals(record.getValue("svFloat"), 123f);
      assertEquals(record.getValue("svDouble"), 123d);
      assertEquals(record.getValue("svString"), "123");
      assertEquals(record.getValue("svBytes"), new ByteArray(new byte[]{123}));
      assertEquals(record.getValue("mvInt"), new Object[]{123});
      assertEquals(record.getValue("mvLong"), new Object[]{123L});
      assertEquals(record.getValue("mvFloat"), new Object[]{123f});
      assertEquals(record.getValue("mvDouble"), new Object[]{123d});
      assertEquals(record.getValue("mvString"), new Object[]{"true"});
    }
  }

  @Test
  public void testTimeTransformer() {
    TimeTransformer transformer = new TimeTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("outgoing"), 123 * 6 * 3600 * 1000L);
    }
  }

  @Test
  public void testDefaultTransformer() {
    CompoundTransformer transformer = CompoundTransformer.getDefaultTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), 123);
      assertEquals(record.getValue("svLong"), 123L);
      assertEquals(record.getValue("svFloat"), 123f);
      assertEquals(record.getValue("svDouble"), 123d);
      assertEquals(record.getValue("svString"), "123");
      assertEquals(record.getValue("svBytes"), new ByteArray(new byte[]{123}));
      assertEquals(record.getValue("mvInt"), new Object[]{123});
      assertEquals(record.getValue("mvLong"), new Object[]{123L});
      assertEquals(record.getValue("mvFloat"), new Object[]{123f});
      assertEquals(record.getValue("mvDouble"), new Object[]{123d});
      assertEquals(record.getValue("mvString"), new Object[]{"true"});
      assertEquals(record.getValue("outgoing"), 123 * 6 * 3600 * 1000L);
    }
  }

  @Test
  public void testPassThroughTransformer() {
    CompoundTransformer transformer = CompoundTransformer.getPassThroughTransformer();
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
    }
  }
}
