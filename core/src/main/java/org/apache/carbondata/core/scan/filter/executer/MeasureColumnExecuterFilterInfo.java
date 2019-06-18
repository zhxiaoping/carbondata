/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.scan.filter.executer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;

public class MeasureColumnExecuterFilterInfo {

  Object[] filterKeys;

  private ByteOpenHashSet byteOpenHashSet;

  private IntOpenHashSet intOpenHashSet;

  private DoubleOpenHashSet doubleOpenHashSet;

  private ShortOpenHashSet shortOpenHashSet;

  private Set<Object> bigDecimalHashSet;

  private LongOpenHashSet longOpenHashSet;

  private BooleanOpenHashSet booleanOpenHashSet;

  private FloatOpenHashSet floatOpenHashSet;

  public void setFilterKeys(Object[] filterKeys, DataType dataType) {
    this.filterKeys = filterKeys;
    if (dataType == DataTypes.BOOLEAN) {
      booleanOpenHashSet = new BooleanOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        booleanOpenHashSet.add(((Boolean) filterKeys[i]).booleanValue());
      }
    }
    if (dataType == DataTypes.BYTE) {
      byteOpenHashSet = new ByteOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        byteOpenHashSet.add(((Byte) filterKeys[i]).byteValue());
      }
    }
    if (dataType == DataTypes.SHORT) {
      shortOpenHashSet = new ShortOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        shortOpenHashSet.add(((Short) filterKeys[i]).shortValue());
      }
    } else if (dataType == DataTypes.INT) {
      intOpenHashSet = new IntOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        intOpenHashSet.add(((Integer) filterKeys[i]).intValue());
      }
    } else if (dataType == DataTypes.FLOAT) {
      floatOpenHashSet = new FloatOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        floatOpenHashSet.add(((Float) filterKeys[i]).floatValue());
      }
    } else if (dataType == DataTypes.LONG) {
      longOpenHashSet = new LongOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        longOpenHashSet.add(((Long) filterKeys[i]).longValue());
      }
    } else if (dataType == DataTypes.DOUBLE) {
      doubleOpenHashSet = new DoubleOpenHashSet();
      for (int i = 0; i < filterKeys.length; i++) {
        doubleOpenHashSet.add(((Double) filterKeys[i]).doubleValue());
      }
    } else {
      this.bigDecimalHashSet = new HashSet<>(Arrays.asList(filterKeys));
    }
  }

  public Object[] getFilterKeys() {
    return filterKeys;
  }

  public ByteOpenHashSet getByteOpenHashSet() {
    return byteOpenHashSet;
  }

  public IntOpenHashSet getIntOpenHashSet() {
    return intOpenHashSet;
  }

  public DoubleOpenHashSet getDoubleOpenHashSet() {
    return doubleOpenHashSet;
  }

  public ShortOpenHashSet getShortOpenHashSet() {
    return shortOpenHashSet;
  }

  public Set<Object> getBigDecimalHashSet() {
    return bigDecimalHashSet;
  }

  public LongOpenHashSet getLongOpenHashSet() {
    return longOpenHashSet;
  }

  public BooleanOpenHashSet getBooleanOpenHashSet() {
    return booleanOpenHashSet;
  }

  public FloatOpenHashSet getFloatOpenHashSet() {
    return floatOpenHashSet;
  }
}
