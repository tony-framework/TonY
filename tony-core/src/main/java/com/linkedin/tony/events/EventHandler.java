/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.events;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyJobMetadata;
import com.linkedin.tony.util.HistoryFileUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class EventHandler extends Thread {
  private static final Log LOG = LogFactory.getLog(EventHandler.class);
  private boolean isStopped = false;
  private BlockingQueue<Event> eventQueue;
  private Path historyFile = null;
  private Path tmpFile = new Path(Constants.TMP_AVRO);
  private DatumWriter<Event> eventWriter = new SpecificDatumWriter<>();
  private DataFileWriter<Event> dataFileWriter = new DataFileWriter<>(eventWriter);
  private OutputStream out;
  private FileSystem myFs;

  public EventHandler(FileSystem fs, BlockingQueue<Event> q) {
    eventQueue = q;
    myFs = fs;
    try {
      out = myFs.create(tmpFile);
      dataFileWriter.create(Event.SCHEMA$, out);
    } catch (IOException e) {
      LOG.error("Failed to set up writer", e);
    }
  }

  @VisibleForTesting
  public void writeEvent(BlockingQueue<Event> queue, DataFileWriter<Event> writer) {
    Event event = null;
    try {
      event = queue.take();
      writer.append(event);
    } catch (IOException e) {
      LOG.error("Failed to append event " + event, e);
    } catch (InterruptedException e) {
      LOG.info("Event writer interrupted", e);
    }
  }

  @VisibleForTesting
  public void drainQueue(BlockingQueue<Event> queue, DataFileWriter<Event> writer) {
    while (!eventQueue.isEmpty()) {
      try {
        Event event = queue.poll();
        writer.append(event);
      } catch (IOException e) {
        LOG.error("Failed to drain queue", e);
      }
    }
  }

  public void emitEvent(Event event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      LOG.error("Failed to add event " + event + " to event queue", e);
    }
  }

  @Override
  public void run() {
    while (!isStopped && !Thread.currentThread().isInterrupted()) {
      writeEvent(eventQueue, dataFileWriter);
    }

    // Clear the queue
    drainQueue(eventQueue, dataFileWriter);

    try {
      dataFileWriter.close();
      out.close();
    } catch (IOException e) {
      LOG.error("Failed to close writer", e);
    }

    // At this point, historyFile should be set
    // If not, then discard all events
    if (historyFile == null) {
      LOG.info("No history file found. Discard all events.");
      try {
        myFs.delete(tmpFile, true);
      } catch (IOException e) {
        LOG.error("Failed to discard all events", e);
      }
      return;
    }

    try {
      myFs.rename(tmpFile, historyFile);
    } catch (IOException e) {
      LOG.error("Failed to rename to jhist file", e);
    }
  }

  public void stop(Path jobDir, TonyJobMetadata metadata) {
    isStopped = true;
    LOG.info("Stopped event handler thread");
    if (jobDir == null) {
      this.interrupt();
      return;
    }
    historyFile = new Path(jobDir, HistoryFileUtils.generateFileName(metadata));
    this.interrupt();
  }
}

