/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.events.ApplicationInited;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventHandler;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.util.HistoryFileUtils;
import com.linkedin.tony.util.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class TestEventHandler {
  private static final Log LOG = LogFactory.getLog(TestEventHandler.class);
  private FileSystem fs = null;
  private BlockingQueue<Event> eventQueue;
  private Thread eventHandlerThread;
  private Event eEventWrapper;
  private ApplicationInited eAppInitEvent = new ApplicationInited("app123", 1, "fakehost");
  private Path jobDir = new Path("./src/test/resources/jobDir");
  private TonyJobMetadata metadata = new TonyJobMetadata();

  private List<Event> parseEvents(FileSystem fs, Path jobDir, TonyJobMetadata metadata) {
    String jhistFile = HistoryFileUtils.generateFileName(metadata);
    if (jhistFile.isEmpty()) {
      return Collections.emptyList();
    }

    Path historyFile = new Path(jobDir, jhistFile);
    List<Event> events = new ArrayList<>();
    try (InputStream in = fs.open(historyFile)) {
      DatumReader<Event> datumReader = new SpecificDatumReader<>(Event.class);
      try (DataFileStream<Event> avroFileStream = new DataFileStream<>(in, datumReader)) {
        Event record = null;
        while (avroFileStream.hasNext()) {
          record = avroFileStream.next(record);
          Event ev = new Event();
          ev.setType(record.getType());
          ev.setEvent(record.getEvent());
          ev.setTimestamp(record.getTimestamp());
          events.add(ev);
        }
      } catch (IOException e) {
        LOG.error("Failed to read events from " + historyFile);
      }
    } catch (IOException e) {
      LOG.error("Failed to open history file", e);
    }
    return events;
  }

  @BeforeClass
  public void setup() {
    HdfsConfiguration conf = new HdfsConfiguration();
    try {
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      fail("Failed setting up FileSystem object");
    }
    eEventWrapper = new Event();
    eEventWrapper.setType(EventType.APPLICATION_INITED);
    eEventWrapper.setEvent(eAppInitEvent);
    eventQueue = new LinkedBlockingQueue<>();
  }

  @Test
  public void testEventHandlerConstructor_failedToSetUpWriter() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.create(any(Path.class))).thenThrow(new IOException("IO Excpt"));

    // we don't need to assign to a variable since we don't use it
    new EventHandler(mockFs, eventQueue);

    verify(mockFs).create(any(Path.class));
  }

  @Test
  public void testEventHandlerE2E_success() throws InterruptedException, IOException {
    fs.mkdirs(jobDir);
    EventHandler eh = new EventHandler(fs, eventQueue);
    eventHandlerThread = new Thread(eh);
    eventHandlerThread.start();
    eh.emitEvent(eEventWrapper);
    eh.stop(jobDir, metadata);
    eventHandlerThread.join();
    List<Event> events = parseEvents(fs, jobDir, metadata);
    Event aEventWrapper = events.get(0);
    ApplicationInited aAppInitEvent = (ApplicationInited) aEventWrapper.getEvent();

    assertEquals(events.size(), 1);
    assertEquals(aAppInitEvent.getApplicationId(), eAppInitEvent.getApplicationId());
    assertEquals(aAppInitEvent.getNumTasks(), eAppInitEvent.getNumTasks());
    assertEquals(aAppInitEvent.getHost(), eAppInitEvent.getHost());
    assertEquals(aEventWrapper.getType(), eEventWrapper.getType());
    assertEquals(aEventWrapper.getTimestamp(), eEventWrapper.getTimestamp());
    assertEquals(fs.listStatus(jobDir).length, 1);

    Utils.cleanupHDFSPath(fs.getConf(), jobDir);
  }

  @Test
  public void testEventHandlerE2E_failedJobDirNotSet() throws InterruptedException, IOException {
    fs.mkdirs(jobDir);
    EventHandler eh = new EventHandler(fs, eventQueue);
    eventHandlerThread = new Thread(eh);
    eventHandlerThread.start();
    eh.stop(null, metadata); // jobDir == null
    eventHandlerThread.join();

    assertEquals(fs.listStatus(jobDir).length, 0);

    Utils.cleanupHDFSPath(fs.getConf(), jobDir);
  }

  @Test
  public void testWriteEvent() throws IOException {
    DataFileWriter<Event> writer = mock(DataFileWriter.class);
    eventQueue.add(eEventWrapper);
    EventHandler eh = new EventHandler(fs, eventQueue);

    assertEquals(eventQueue.size(), 1);
    eh.writeEvent(eventQueue, writer); // should drain the queue
    assertEquals(eventQueue.size(), 0);
    verify(writer).append(eEventWrapper);
  }

  @AfterClass
  public void cleanUp() throws IOException {
    fs.delete(new Path(Constants.TMP_AVRO), true);
  }
}
