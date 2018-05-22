/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestSkipBigRowScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestSkipBigRowScanner.class);

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static int valueWidth = 2 * 1024 * 1024;

  @BeforeClass
  public static void before() throws Exception {
    HTU.getConfiguration().setLong(HConstants.TABLE_MAX_ROWSIZE_KEY,
            1024 * 1024L);
    HTU.getConfiguration().setBoolean(HConstants.TABLE_SKIP_BIGROWS_KEY,
            true);
    HTU.startMiniCluster();
  }

  @AfterClass
  public static void after() throws Exception {
    HTU.shutdownMiniCluster();
  }


  @Test
  public void testSkipBigRow() throws Exception {
    final TableName tableName = TableName.valueOf("testSkipBigRow");
    final byte[] cf_name = Bytes.toBytes("a");
    final byte[] col_name = Bytes.toBytes("a");

    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(cf_name).build();
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tableDescriptorBuilder.setColumnFamily(cfd);
    TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
    HTU.getAdmin().createTable(tableDescriptor);
    Table ht = HTU.getConnection().getTable(tableName);

    byte[] val_large = new byte[valueWidth];

    List<Put> puts = new ArrayList<>();
    Put put = new Put(Bytes.toBytes("0"));
    put.addColumn(cf_name, col_name, val_large);
    puts.add(put);

    put = new Put(Bytes.toBytes("1"));
    put.addColumn(cf_name, col_name, Bytes.toBytes("small"));
    puts.add(put);

    put = new Put(Bytes.toBytes("2"));
    put.addColumn(cf_name, col_name, val_large);
    puts.add(put);

    ht.put(puts);
    puts.clear();

    Scan scan = new Scan();
    scan.addColumn(cf_name, col_name);
    ResultScanner result_scanner = ht.getScanner(scan);
    Result res;
    long rows_count = 0;
    //Only 1 row
    while ((res = result_scanner.next()) != null) {
      Assert.assertEquals("1", Bytes.toString(res.getRow()));
      rows_count++;
    }

    Assert.assertEquals(1, rows_count);
    result_scanner.close();
    ht.close();
  }

  @Test
  public void testSkipRowInJoinedHeap() throws IOException {
    final TableName tableName = TableName.valueOf("testSkipRowInJoinedHeap");
    final byte[] essential_cf = Bytes.toBytes("essential");
    final byte[] joined_cf = Bytes.toBytes("joined");
    final byte[] col_name = Bytes.toBytes("a");
    final byte[] flag_yes = Bytes.toBytes("Y");


    ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(essential_cf);
    ColumnFamilyDescriptor essential = cfd.build();
    ColumnFamilyDescriptor joined = ColumnFamilyDescriptorBuilder.newBuilder(joined_cf).build();

    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tableDescriptorBuilder.setColumnFamily(essential);
    tableDescriptorBuilder.setColumnFamily(joined);

    TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
    HTU.getAdmin().createTable(tableDescriptor);
    Table ht = HTU.getConnection().getTable(tableName);

    byte[] val_large = new byte[valueWidth];
    List<Put> puts = new ArrayList<>();
    Put put = new Put(Bytes.toBytes("0"));
    put.addColumn(essential_cf, col_name, flag_yes);
    put.addColumn(joined_cf, col_name, val_large);
    puts.add(put);

    put = new Put(Bytes.toBytes("1"));
    put.addColumn(essential_cf, col_name, flag_yes);
    put.addColumn(joined_cf, col_name, Bytes.toBytes("small"));
    puts.add(put);

    put = new Put(Bytes.toBytes("2"));
    put.addColumn(essential_cf, col_name, flag_yes);
    put.addColumn(joined_cf, col_name, val_large);
    puts.add(put);

    ht.put(puts);
    puts.clear();

    Scan scan = new Scan();
    scan.addColumn(essential_cf, col_name);
    scan.addColumn(joined_cf, col_name);

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
            essential_cf, col_name, CompareOperator.EQUAL, flag_yes);
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    scan.setLoadColumnFamiliesOnDemand(true);

    ResultScanner result_scanner = ht.getScanner(scan);
    Result res;
    long rows_count = 0;
    //Only 1 row
    while ((res = result_scanner.next()) != null) {
      Assert.assertEquals("1", Bytes.toString(res.getRow()));
      rows_count++;
    }

    Assert.assertEquals(1, rows_count);
    result_scanner.close();
    ht.close();
  }
}
