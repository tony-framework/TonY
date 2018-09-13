/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.io.HdfsAvroFileSplitReader;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TestReader {
  private static final int NUM_RECORD = 100000;

  /**
   * Tests HdfsAvroFileSplitReader calculating offset correctly. Specifically
   * given a specific total length, a max id, HdfsAvroFileSplitReader can
   * get the correct byte range for each of the id. "correct" here means the
   * byte range are non-overlapping and covers the entire range.
   */
  @Test
  public void testOffsetCalculation() {
    for (int t = 0; t < 1000; t++) {
      Random ran = new Random();
      long totalLen = Math.abs(ran.nextLong()) % 100000 + 10000;
      int totalIdx = ran.nextInt(20) + 10; // make sure this is not 0

      long next_start = 0;

      for (int i = 0; i < totalIdx; i++) {
        long start = HdfsAvroFileSplitReader.computeReadSplitStart(
            totalLen, i, totalIdx);
        assertEquals(next_start, start);
        long length = HdfsAvroFileSplitReader
            .computeReadSplitLength(totalLen, i, totalIdx);
        next_start = start + length;
      }
      assertEquals(totalLen, next_start);
    }
  }

  /**
   * Tests HdfsAvroFileSplitReader can read multiple avro files correctly.
   */
  @Test
  public void testReader() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    String path0 = "testReader0.avro";
    String path1 = "testReader1.avro";
    String path2 = "testReader2.avro";
    Files.deleteIfExists(Paths.get(path0));
    Files.deleteIfExists(Paths.get(path1));
    Files.deleteIfExists(Paths.get(path2));
    try {
      Schema schema = generateTestSchema();
      List<GenericData.Record> all_records = new ArrayList<>();
      all_records.addAll(generateTestAvro(path0, schema));
      all_records.addAll(generateTestAvro(path1, schema));
      all_records.addAll(generateTestAvro(path2, schema));
      // NOTE This will not use HDFS, this generates a RawLocalFileSystem
      // instance, we will only be testing with local file system in this unit
      // test.
      HdfsAvroFileSplitReader reader =
          new HdfsAvroFileSplitReader(conf, Arrays.asList(path0, path1, path2),
              0, 1);

      // check schema can be correctly read
      String schemaJson = reader.getSchemaJson();
      Schema readSchema = new Schema.Parser().parse(schemaJson);
      assertEquals(schema, readSchema);

      readAndCheck(reader, readSchema, all_records);
      assertEquals(all_records.size(), 0);
      reader.close();
    } finally {
      Files.deleteIfExists(Paths.get(path0));
      Files.deleteIfExists(Paths.get(path1));
      Files.deleteIfExists(Paths.get(path2));
    }
  }

  /**
   * Tests HdfsAvroFileSplitReader can read avro files split correctly.
   * Specifically test several readers reading several files.
   */
  @Test
  public void testReaderPartialRead() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    String path0 = "testReader0.avro";
    String path1 = "testReader1.avro";
    String path2 = "testReader2.avro";
    Files.deleteIfExists(Paths.get(path0));
    Files.deleteIfExists(Paths.get(path1));
    Files.deleteIfExists(Paths.get(path2));
    try {
      Schema schema = generateTestSchema();

      List<GenericData.Record> records0 = generateTestAvro(path0, schema);
      List<GenericData.Record> records1 = generateTestAvro(path1, schema);
      List<GenericData.Record> records2 = generateTestAvro(path2, schema);

      List<GenericData.Record> all_records = new ArrayList<>();
      all_records.addAll(records0);
      all_records.addAll(records1);
      all_records.addAll(records2);

      // NOTE here we have 3 avro files, and a total of 3 splits.
      // but it does not necessarily mean each reader will be processing
      // exactly one file, because the files are different, and a small
      // difference could cause the avro file sync point to be far off.
      HdfsAvroFileSplitReader reader0 =
          new HdfsAvroFileSplitReader(conf, Arrays.asList(path0, path1, path2),
              0, 3);
      // wait a bit for the thread to start and load the schema
      Thread.sleep(100);
      String schemaJson = reader0.getSchemaJson();
      Schema readSchema = new Schema.Parser().parse(schemaJson);
      assertEquals(schema, readSchema);
      // this call below will remove some entries from all_records.
      readAndCheck(reader0, readSchema, all_records);
      reader0.close();

      HdfsAvroFileSplitReader reader1 =
          new HdfsAvroFileSplitReader(conf, Arrays.asList(path0, path1, path2),
              1, 3);
      // wait a bit for the thread to start and load the schema
      Thread.sleep(100);
      schemaJson = reader1.getSchemaJson();
      readSchema = new Schema.Parser().parse(schemaJson);
      assertEquals(schema, readSchema);
      readAndCheck(reader1, readSchema, all_records);
      reader1.close();

      HdfsAvroFileSplitReader reader2 =
          new HdfsAvroFileSplitReader(conf, Arrays.asList(path0, path1, path2),
              2, 3);
      // wait a bit for the thread to start and load the schema
      Thread.sleep(100);
      schemaJson = reader2.getSchemaJson();
      readSchema = new Schema.Parser().parse(schemaJson);
      assertEquals(schema, readSchema);
      readAndCheck(reader2, readSchema, all_records);
      reader2.close();

      // after all three readers, all records should be removed.
      assertEquals(all_records.size(), 0);
    } finally {
      Files.deleteIfExists(Paths.get(path0));
      Files.deleteIfExists(Paths.get(path1));
      Files.deleteIfExists(Paths.get(path2));
    }
  }

  private void readAndCheck(HdfsAvroFileSplitReader reader, Schema readSchema,
      List<GenericData.Record> all_records) throws IOException, InterruptedException {
    while (reader.hasNext()) {
      // an arbitrary chosen batch size, to capture the more likely case
      // that last batch will be smaller
      List<ByteBuffer> records = reader.nextBatchBytes(900);

      for (ByteBuffer recordBytes : records) {
        byte[] buffer = recordBytes.array();
        BinaryDecoder decoder =
            DecoderFactory.get().binaryDecoder(buffer, null);
        GenericDatumReader<GenericRecord> datumReader =
            new GenericDatumReader<>(readSchema);
        GenericRecord record = datumReader.read(null, decoder);
        String name = record.get("name").toString();
        int age = (Integer) record.get("age");

        GenericData.Record expected = all_records.remove(0);

        assertEquals(name, expected.get("name"));
        assertEquals(age, expected.get("age"));
      }
    }
  }

  private Schema generateTestSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Field("name", Schema.create(Schema.Type.STRING),
        null, "default"));
    fields.add(new Field("age", Schema.create(Schema.Type.INT),
        null, -1));

    Schema schema = Schema.createRecord("Person", null, null, false);
    schema.setFields(fields);

    return schema;
  }

  private List<GenericData.Record> generateTestAvro(
      String path, Schema schema) throws IOException {

    Random ran = new Random();
    File file = new File(path);

    List<GenericData.Record> all_records = new ArrayList<>();

    GenericDatumWriter<GenericData.Record> datum =
        new GenericDatumWriter<>(schema);
    DataFileWriter<GenericData.Record> writer =
        new DataFileWriter<>(datum);

    writer.create(schema, file);

    for (int i = 0; i < NUM_RECORD; i ++) {
      GenericData.Record record =
          makeObject(schema, "Person " + i, ran.nextInt(120));
      all_records.add(record);
      writer.append(record);
    }
    writer.close();
    return all_records;
  }

  private static GenericData.Record makeObject(Schema schema, String name,
      int age) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("name", name);
    record.put("age", age);
    return(record);
  }
}
