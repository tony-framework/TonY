/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.events;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.models.JobMetadata;
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

  private volatile boolean isStopped = false;

  private BlockingQueue<Event> eventQueue;
  private Path finalHistFile = null;
  private Path inProgressHistFile = null;
  private DatumWriter<Event> eventWriter = new SpecificDatumWriter<>();
  private DataFileWriter<Event> dataFileWriter = new DataFileWriter<>(eventWriter);
  private OutputStream out;
  private FileSystem myFs;

  // Call the constructor to initialize the queue and fs object,
  // and then call setUpThread with the appropriate parameters
  // to set up destination path for event writer
  public EventHandler(FileSystem fs, BlockingQueue<Event> q) {
    eventQueue = q;
    myFs = fs;
  }

  public void setUpThread(Path intermDir, JobMetadata metadata) {
    if (intermDir == null) {
      return;
    }
    inProgressHistFile = new Path(intermDir, HistoryFileUtils.generateFileName(metadata));
    try {
      out = myFs.create(inProgressHistFile);
      dataFileWriter.create(Event.SCHEMA$, out);
      LOG.info("Writing events to " + inProgressHistFile);
    } catch (IOException e) {
      LOG.error("Failed to set up writer", e);
    }
  }

  @VisibleForTesting
  void writeEvent(BlockingQueue<Event> queue, DataFileWriter<Event> writer) {
    Event event = null;
    try {
      event = queue.take();
      writer.append(event);
    } catch (IOException e) {
      LOG.error("Failed to append event " + event, e);
    } catch (InterruptedException e) {
      LOG.info("Event writer interrupted");
    }
  }

  @VisibleForTesting
  void drainQueue(BlockingQueue<Event> queue, DataFileWriter<Event> writer) {
    LOG.info("Draining queue");
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
      LOG.info("Emitting event: " + event);
      eventQueue.put(event);
    } catch (InterruptedException e) {
      LOG.error("Failed to add event " + event + " to event queue", e);
    }
  }

  @Override
  public void run() {
    LOG.info("Checking if jhist file is ready...");
    // If setupThread method fails to create inProgressHistFile,
    // return immediately since we don't have any file to begin with
    if (inProgressHistFile == null) {
      LOG.warn("inProgressHistFile is null");
      return;
    }

    while (!isStopped && !Thread.currentThread().isInterrupted()) {
      writeEvent(eventQueue, dataFileWriter);
    }

    // Clear the queue
    drainQueue(eventQueue, dataFileWriter);
  }

  private void cleanUp() {
    try {
      dataFileWriter.close();
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
      LOG.error("Failed to close writer", e);
    }
  }

  private void moveInProgressToFinal() {
    try {
      LOG.info("Moving " + inProgressHistFile + " to " + finalHistFile + ".");
      if (!myFs.rename(inProgressHistFile, finalHistFile)) {
        LOG.error("Failed to move " + inProgressHistFile + " to " + finalHistFile);
      }
    } catch (IOException e) {
      LOG.error("Failed to rename to jhist file", e);
    }
  }

  public void stop(Path intermDir, JobMetadata metadata) {
    if (inProgressHistFile == null) {
      LOG.warn("inProgressHistFile is null");
      return;
    }

    finalHistFile = new Path(intermDir, HistoryFileUtils.generateFileName(metadata));
    LOG.info("Stopping event handler thread");
    isStopped = true;
    this.interrupt();
    try {
      this.join();
    } catch (InterruptedException e) {
      LOG.warn("Encountered interruption while stopping event handler thread", e);
    }

    cleanUp();
    moveInProgressToFinal();
  }
}

