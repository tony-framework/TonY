/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.io;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.util.Utils;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * To read avro file which is stored on a remote hdfs uri. The reader allows
 * taking an offset range (specified by start offset and length), then the
 * reader will locate and *try to* read only the records in the given byte
 * range. Note that it is not precisely the byte range because avro record must
 * be complete. This call utilize the internal block of avro file itself. So
 * no matter what offset is given, always complete avro block(s) will be
 * returned.
 *
 * Some examples, if avro file itself has three blocks, offset 0-1000 and
 * 1000-2000, 2000-3000.
 *
 * 1. Reading bytes 900 to 1500 will actually locate to the next block
 * boundary after 900, which is at 1000. Then read up to the next boundary,
 * so this actually reads offset 1000 - 2000.
 *
 * 2. Reading bytes 600 to 700 will effectively do nothing because locating to
 * the block boundary 1000 will already be after the end offset of 700.
 *
 * 3. Reading bytes 500 to 2100 will similarly, actually read two blocks in
 * range 1000 - 3000.
 *
 * This is also how MR handles avro input split.
 * The block boundary is specific to each avro file and it's written so there
 * is no universally best range. But as long as byte splits given to workers
 * are non-overlapping and cover the entire range, there will be no double
 * processing or missing. The load unbalance is no larger than one avro block
 * anyway.
 *
 * One remote avro file corresponds to one reader instance on each task.
 * There are two key calls that should called by the py4j client side.
 *
 * nextBatchBytes(batchSize) returns a list of avro records in bytes
 * getSchemaJson() returns the schema of the avro file, in string
 * close() close the stream and terminates the fetcher thread.
 *
 *
 * This class can read multiple files. For given 4 files of size:
 * 100, 200, 100, 400
 * with 4 readers in total. Since the total number of bytes is 800, the 2nd
 * reader with id 1 will try to read the byte range 200 - 399. Effectively
 * reading half the second file and the whole third file (again it will not be
 * precisely these offsets due to avro block boundary).
 *
 * This class exposes APIs for python code to leverage through py4j.
 * To use this class, python code first calls getSchemaJson() to get
 * the schema of the avro file, in the format of a json string. This
 * is needed for python code to decode data into original Avro record.
 *
 * Then python code can keep calling nextBatch to read through the
 * entire dataset.
 *
 * This class currently provides three version of nextBatch call,
 * they are different only in terms of underlying implementation.
 * Thus coming with different pros and cons. Here we assume python
 * uses fastavro for Avro decoding.:
 * 1. nextBatchBytes(batchSize) returns records as a list of byte
 *    arrays. Each byte array represents one single avro records.
 *    This is the most intuitive and with smallest overhead, but
 *    also the slowest one. As python code needs decode record
 *    by record, which is slow if using fastavro or pyavro.
 * 2. nextBatchFile(batchSize) returns one batch of records as
 *    a complete Avro file, the Avro file is only in memory and
 *    can be treated as reading all bytes of a complete Avro file
 *    from disk then the bytes get fed into python code. This
 *    improves performance compared to 1, as fastavro performs
 *    much better when processing a batch of records. But this
 *    requires a higher memory settings to hold the batch in-mem
 *    file. Transferring the batch through py4j also seems
 *    expensive. The returned in-memory Avro file is in the format
 *    of a FileObject instance.
 * 3. nextBatchFileLocalSpill(batchSize) returns a path to a
 *    file in local filesystem, pointing to a file written to
 *    disk. The file is the Avro batch, in form of a complete
 *    Avro file. The only difference compared with 2 is that the file
 *    is transferred through local disk rather than py4j. The
 *    python code will need to find the disk location, read and
 *    process the Avro file. This is due to that even local file
 *    IO seems faster then using py4j here. So this approach
 *    is fastest as of now, and no longer requires much memory.
 *    But consuming disk space. Also NOTE this approach requires
 *    Python code to call notifyFinish(path) when it finishes
 *    processing one file.
 */
public class HdfsAvroFileSplitReader implements Closeable {

  private static final Log LOG = LogFactory.getLog(HdfsAvroFileSplitReader.class);

  /**
   * TODO currently, A single fetcher thread, revisit adding more.
   * The single fetcher thread will iterate all the given file sections.
   */
  private final Thread fetcherThread;
  private final DataFetcher fetcher;
  private final Set<String> localBufferFiles;
  private boolean shouldStop;

  private final int maxBufferCapacity;

  /* If using local file as data transfer, one file
   * will be created for one batch. This number controls the max
   * number of such local batch files. This is to prevent those
   * files from exploding local disk.
   *
   * Effectively this may not be needed because the training
   * usually proceed with one thread, one batch at a time,
   * in which case no more than one file should be needed.
   */
  // TODO : make below more configurable
  private static final int MAX_LOCAL_FILE_NUM = 50;
  private static final int MAX_BUFFER_CAPACITY_DEFAULT = 1024;
  private static final double POLL_THRESHOLD = 0.8;
  /*
   * Used by random shuffle only. The threshold
   * to start reading data. e.g. if the buffer
   * is 1000, and threshold is 0.8, then only
   * when there are >= 1000*0.8 = 800 entries
   * in the buffer will a random record be read.
   * This guarantees a record is random among
   * 800 records.
   */
  private final double pollingThreshold;

  private final InternalBuffer<GenericRecord> buffer;

  class DataFetcher implements Runnable {
    private Schema schema;
    private final FileSystem fs;
    private final List<FileAccessInfo> fileAccessInfos;

    // indicate ALL read is done.
    boolean readFinished;

    DataFetcher(FileSystem fs, List<FileAccessInfo> fileAccessInfos) {
      this.schema = null;
      this.fileAccessInfos = fileAccessInfos;
      this.fs = fs;
    }

    @Override
    public void run() {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get()
          .binaryEncoder(stream, null);

      try {
        for (int i = 0; i < fileAccessInfos.size(); i++) {
          FileAccessInfo info = fileAccessInfos.get(i);

          Path inputPath = new Path(info.filePath);
          FSDataInputStream inputStream = fs.open(inputPath);
          this.readFinished = false;
          long startOffset = info.startOffset;
          long readLength = info.readLength;

          SeekableInput seekableInput = new SeekableInput() {
            @Override
            public void seek(long offset) throws IOException {
              inputStream.seek(offset);
            }

            @Override
            public long tell() throws IOException {
              return inputStream.getPos();
            }

            @Override
            public long length() {
              return info.fileLength;
            }

            @Override
            public int read(byte[] bytes, int offset, int length)
                throws IOException {
              return inputStream.read(bytes, offset, length);
            }

            @Override
            public void close() throws IOException {
              inputStream.close();
            }
          };
          FileReader<GenericData.Record> dataFileReader = DataFileReader.openReader(
              seekableInput, new GenericDatumReader<GenericData.Record>());
          // a quick sanity check. Schema of all input files should be the same
          // although even if they don't this should still work.
          if (this.schema != null
              && !this.schema.equals(dataFileReader.getSchema())) {
            LOG.warn("Input file have different schema");
          }
          this.schema = dataFileReader.getSchema();
          dataFileReader.sync(startOffset);

          DatumWriter<GenericRecord> datumWriter =
              new GenericDatumWriter<>(schema);

          while (!shouldStop) {
            try {
              if (dataFileReader.hasNext() && !dataFileReader.pastSync(
                  startOffset + readLength)) {
                GenericData.Record currentRecord =
                    dataFileReader.next(null);
                datumWriter.write(currentRecord, encoder);
                encoder.flush();
                stream.reset();
                buffer.put(currentRecord);
              } else {
                LOG.info("Finished Processing a segment " + i + " remaining "
                    + (fileAccessInfos.size() - 1 - i));
                break;
              }
            } catch (IOException ioe) {
              LOG.error("Error fetching avro file", ioe);
              break;
            } catch (InterruptedException ie) {
              LOG.debug("Fetcher interrupted", ie);
            }
          }
        }
        LOG.info("Read finished");
        readFinished = true;
      } catch (IOException ioe) {
        LOG.error("Fetcher failed", ioe);
      } finally {
        try {
          stream.close();
        } catch (IOException ioe) {
          LOG.error("Error terminating fetcher", ioe);
        }
      }
    }
  }


  @VisibleForTesting
  public static long computeReadSplitStart(long totalLength, int idx,
      int totalIdx) {
    return idx  * totalLength / totalIdx;
  }

  @VisibleForTesting
  public static long computeReadSplitLength(long totalLength, int idx,
      int totalIdx) {
    long nextStart = (idx + 1) * totalLength / totalIdx;
    return Math.min(nextStart, totalLength)
        - computeReadSplitStart(totalLength, idx, totalIdx);
  }

  public HdfsAvroFileSplitReader(Configuration conf, List<String> readPaths,
      int splitId, int numOfReaders) throws IOException {
    this(FileSystem.get(conf), readPaths, splitId, numOfReaders, false);
  }

  @VisibleForTesting
  public HdfsAvroFileSplitReader(Configuration conf, List<String> readPaths,
      int splitId, int numOfReaders, boolean useRandomShuffle) throws IOException {
    this(FileSystem.get(conf), readPaths, splitId, numOfReaders, useRandomShuffle);
  }

  public HdfsAvroFileSplitReader(Configuration conf, List<String> readPaths,
      int splitId, int numOfReaders, int maxBufferCapacity, boolean useRandomShuffle,
      double pollingThreshold) throws IOException {
    this(FileSystem.get(conf), readPaths, splitId, numOfReaders,
        maxBufferCapacity, useRandomShuffle, pollingThreshold);
  }

  HdfsAvroFileSplitReader(FileSystem fs, List<String> readPaths,
      int splitId, int numOfReaders, boolean useRandomShuffle) throws IOException {
    this(fs, readPaths, splitId, numOfReaders, MAX_BUFFER_CAPACITY_DEFAULT,
        useRandomShuffle, POLL_THRESHOLD);
  }

  /**
   * Create a HdfsAvroFileSplitReader instance. If useRandomShuffle is set to
   * true, pollingThreshold will be used to control the level of randomness.
   * For example, if max buffer capacity is 1000, polling threshold is set to
   * 0.8, then a record read will hold until there are at least 1000 * 0.8 = 800
   * entries in the buffer, then a random one of the 800 will be chosen. This
   * controls the level randomness to be always 1/800. (In the case there are
   * not 800 records remaining in total, this won't take effect).
   *
   * @param fs the Hadoop FileSystem instance
   * @param readPaths the list of file path to read.
   * @param splitId the id of this split reader, relative to all the reader
   *                instance of the same input list. For example, if there are 10
   *                instances across workers, each worker has id
   *                0, 1, 2..., which must be given to the reader.
   * @param numOfReaders the total number of readers for the input list. For
   *                     example, if there are 10 instances across workers, each
   *                     reader must be given the number 10.
   * @param maxBufferCapacity the max buffer size.
   * @param useRandomShuffle whether to enable random shuffle
   * @param pollingThreshold the polling threshold, used along with random shuffle.
   *
   * @throws IOException
   */
  public HdfsAvroFileSplitReader(FileSystem fs, List<String> readPaths,
      int splitId, int numOfReaders, int maxBufferCapacity, boolean useRandomShuffle,
      double pollingThreshold) throws IOException {
    this.shouldStop = false;
    LOG.info("Using random shuffle? " + useRandomShuffle);
    this.maxBufferCapacity = maxBufferCapacity;
    this.pollingThreshold = pollingThreshold;
    this.buffer = new InternalBuffer<>(useRandomShuffle, maxBufferCapacity);
    this.localBufferFiles = ConcurrentHashMap.newKeySet();

    long totalLength = 0;
    List<Long> allFileLength = new ArrayList<>();

    for (String readPath : readPaths) {
      long fileLength = getFileLength(fs, readPath);
      totalLength += fileLength;
      allFileLength.add(fileLength);
    }

    long startOffset =
        computeReadSplitStart(totalLength, splitId, numOfReaders);
    long readLength =
        computeReadSplitLength(totalLength, splitId, numOfReaders);

    List<FileAccessInfo> fileAccessInfos = createReadInfo(readPaths,
        allFileLength, startOffset, readLength);
    LOG.info("Initialization, creating fetcher");
    this.fetcher = new DataFetcher(fs, fileAccessInfos);
    this.fetcherThread = new Thread(this.fetcher);
    this.fetcherThread.start();
  }

  public List<FileAccessInfo> createReadInfo(List<String> readPaths,
      List<Long> allFileLength, long startOffset, long readLength) {

    List<FileAccessInfo> filesToRead = new ArrayList<>();

    long accumulate = 0;
    int targetStartFileIdx = -1;
    long targetStartFileOffset = -1;
    for (int i = 0; i < allFileLength.size(); i++) {
      long currentStart = accumulate;
      long currentEnd = accumulate + allFileLength.get(i);
      if (currentStart <= startOffset && currentEnd > startOffset) {
        targetStartFileIdx = i;
        targetStartFileOffset = startOffset - accumulate;
        break;
      }
      accumulate += allFileLength.get(i);
    }

    if (targetStartFileIdx == -1 || targetStartFileOffset == -1) {
      throw new RuntimeException("Could not locate the file to read with "
          + "starting offset:" + startOffset);
    }

    while (readLength > 0) {
      String fileName = readPaths.get(targetStartFileIdx);
      long fileLength = allFileLength.get(targetStartFileIdx);
      long actualReadLen = Math.min(
          readLength, fileLength - targetStartFileOffset);
      filesToRead.add(new FileAccessInfo(fileName, targetStartFileOffset,
          actualReadLen, fileLength));
      targetStartFileIdx += 1;
      targetStartFileOffset = 0;
      readLength -= actualReadLen;
    }
    LOG.debug("File info created " + filesToRead.size());
    return filesToRead;
  }

  private class FileAccessInfo {
    final String filePath;
    final long startOffset;
    final long readLength;
    final long fileLength;

    FileAccessInfo(String filePath, long startOffset, long readLength,
        long fileLength) {
      this.filePath = filePath;
      this.startOffset = startOffset;
      this.readLength = readLength;
      this.fileLength = fileLength;
    }

    @Override
    public String toString() {
      return "AccessInfo:" + filePath + ":" + startOffset + ":"
          + readLength + ":" + fileLength;
    }
  }

  private long getFileLength(FileSystem fs, String inputPathStr)
      throws IOException {
    Path inputPath = new Path(inputPathStr);
    FileStatus status = fs.getFileStatus(inputPath);
    return status.getLen();
  }

  public String getSchemaJson() {
    /*
     There can be a race condition here:
     When this method is called, it is possible that the fetcher schema
     has not been read yet. This is because fetcher thread will be reading
     the schema and it could take some time. If someone tries to get
     schema before fetcher reads the it, a null schema will be returned.
     Which may cause issue to the caller. Add a sleep spin loop to wait
     for the schema to be ready, or throw exception if the schema is
     still not ready after 10 seconds.
     */
    int attempt = 0;
    this.fetcher.schema = Utils.pollTillNonNull(() -> this.fetcher.schema, 1, 60);
    if (this.fetcher.schema == null) {
      throw new RuntimeException("Could not get schema string");
    }
    return this.fetcher.schema.toString();
  }

  /**
   * An utility class. This is the class exposed through
   * py4j for Python part to access the records. The sole
   * external access point of the buffer.
   *
   * Specifically, one {@link FileObject} instance can be viewed as
   * one single, complete avro file, maintained in memory
   * as a byte array. Python code can simply parse this
   * in memory byte array as if it is a regular avro file.
   */
  class FileObject extends OutputStream {

    ByteArrayOutputStream stream;

    FileObject() {
      // the 1 MB here is only the initial size, based on
      // estimation.
      stream = new ByteArrayOutputStream(1024 * 1024);
    }

    @Override
    public void write(int b) throws IOException {
      stream.write(b);
    }

    public byte[] readAll() {
      return stream.toByteArray();
    }
  }

  /**
   * Called by external python code to generate a FileObject.
   * One batch will lead to one FileObject being created.
   * @param batchSize the batch size
   * @return the FileObject instance for this batch
   * @throws IOException
   * @throws InterruptedException
   */
  public FileObject nextBatchFile(int batchSize)
      throws IOException, InterruptedException {
    if (!hasNext()) {
      LOG.warn("End of input, no more batch to read");
    }
    FileObject fo = new FileObject();
    try (DataFileWriter<Object> writer = new DataFileWriter<>(
        new GenericDatumWriter<>(fetcher.schema))) {
      writer.create(fetcher.schema, fo);
      writeBatch(writer, batchSize);
    }
    return fo;
  }

  /**
   * Called by external python code to generate a local batch file.
   * One batch will lead to one batch file being created.
   * @param batchSize the batch size
   * @return the path to the batch file of this batch
   * @throws IOException
   * @throws InterruptedException
   */
  public String nextBatchFileLocalSpill(int batchSize)
      throws IOException, InterruptedException {
    if (!hasNext()) {
      LOG.warn("End of input, no more batch to read");
    }
    String localPath = getLocalOutputPath();
    File file = new File(localPath);
    try (DataFileWriter<Object> writer = new DataFileWriter<>(
        new GenericDatumWriter<>(fetcher.schema))) {
      writer.create(fetcher.schema, file);
      writeBatch(writer, batchSize);
    }
    while (localBufferFiles.size() >= MAX_LOCAL_FILE_NUM) {
      Thread.sleep(10);
    }
    localBufferFiles.add(localPath);
    return localPath;
  }

  private void writeBatch(DataFileWriter<Object> writer,
      int batchSize)
      throws IOException, InterruptedException {
    for (int i = 0; i < batchSize; i++) {
      GenericRecord record = null;
      int trial = 100;
      while (record == null && trial-- >= 0 && hasNext()) {
        record = buffer.poll(100, TimeUnit.MILLISECONDS);
      }
      if (hasNext() && record == null) {
        throw new IOException(
            "Unable to retrieve a single record after 10 seconds");
      }
      if (record != null) {
        writer.append(record);
      } else {
        // this means there is no more data to read.
        break;
      }
    }
  }

  private String getLocalOutputPath() {
    return "tmp_out-" + UUID.randomUUID().toString();
  }

  /**
   * When the processing of the local shared file is done, the caller (from
   * python side) needs to inform this reader to delete local spill file
   * and remove it from local buffer list. NOTE: python caller failing to
   * do so will not leave the files exist forever as long as this reader's
   * close() gets called, but will limit the cap of how many files it can
   * open at the same time.
   *
   * TODO : add a way to clean the files on disk even when the reader
   * failed with exception.
   * @param path a path to a local buffer file that has been processed
   * @throws IOException
   */
  public void notifyFinish(String path) throws IOException {
    localBufferFiles.remove(path);
    Files.deleteIfExists(Paths.get(path));
  }

  /**
   * Called by external python code to generate a list of byte
   * sequence. (ByteBuffer in Java, bytes in Python).
   * One batch will lead to one list being created. One record
   * per byte buffer.
   * @param batchSize the batch size
   * @return the list of the records in ByteBuffer for this batch
   * @throws IOException
   * @throws InterruptedException
   */
  public List<ByteBuffer> nextBatchBytes(int batchSize)
      throws IOException, InterruptedException {
    LOG.debug("Next batch called:" + batchSize);
    List<ByteBuffer> ret = new LinkedList<>();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get()
        .binaryEncoder(stream, null);

    while (ret.size() < batchSize) {
      /*
       There is a race condition here:
       when buffer is empty, may first checks and finds that
       fetcher.readFinished so this thread will expect further data.
       but after this check, fetcher thread changes readFinished to false,
       (no more data will come). Then this thread will block as no data will
       come. We may just change to a timed wait. So that this thread waits
       a while and again check, it will finds that readFinished is true also,
       then it can break. 100ms wait is expensive, but this may only happen
       once at the very end of the read. So should be fine.
       */
      if (buffer.isEmpty() && fetcher.readFinished) {
        break;
      }
      GenericRecord record = buffer.poll(100, TimeUnit.MILLISECONDS);

      if (record != null) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(fetcher.schema);
        datumWriter.write(record, encoder);
        encoder.flush();

        ByteBuffer data = ByteBuffer.wrap(stream.toByteArray());
        stream.reset();
        ret.add(data);
      }
    }
    return ret;
  }

  public boolean hasNext() {
    return !buffer.isEmpty() || !fetcher.readFinished;
  }

  @Override
  public void close() {
    this.shouldStop = true;
    if (!fetcher.readFinished) {
      // the fetcher maybe blocked on writing to the buffer (buffer is full)
      // interrupt it awakes it in ours case.
      this.fetcherThread.interrupt();
    }
    if (localBufferFiles != null && !localBufferFiles.isEmpty()) {
      // getting here means the training is closing the reader while there
      // is still data.
      LOG.warn("Unprocessed local buffer file. Training ended prematurely?");
      // cleaning up local buffer files.
      for (String path : localBufferFiles) {
        try {
          Files.deleteIfExists(Paths.get(path));
        } catch (IOException ioe) {
          LOG.error("Getting exception when processing local buffer file " + path
              + " still proceeding.", ioe);
        }
      }
    }
  }

  /**
   * Internal representation of the read buffer. Underlying,
   * uses either a blocking queue (for sequential read) or
   * a list (for random shuffle read). But only one will be
   * used, specified in constructor. Could be using list for
   * both cases, but blocking queue comes with better
   * performance.
   *
   * Sequential read with blocking queue comes with smaller
   * memory usage and more performance. Random shuffle read
   * will return records with certain randomness, but comes
   * with a higher cost of memory and time.
   * @param <T> the type of records in the buffer
   */
  class InternalBuffer<T> {

    private final boolean useRandomShuffle;

    // blocking queue for sequential read
    private final BlockingQueue<T> queue;

    // a list with lock, and with random removal
    private final ArrayList<T> list;
    private final ReentrantLock lock;
    private final Random ran;

    private final Condition notFull;
    private final Condition bufferReady;

    // the max size of the buffer, for both
    // queue and list
    private final int bufferSize;

    InternalBuffer(boolean useRandomShuffle, int bufferSize) {
      this.useRandomShuffle = useRandomShuffle;
      this.bufferSize = bufferSize;
      this.lock = new ReentrantLock();
      this.notFull = lock.newCondition();
      this.bufferReady = lock.newCondition();

      if (useRandomShuffle) {
        list = new ArrayList<>();
        ran = new Random();
        queue = null;
      } else {
        list = null;
        ran = null;
        queue = new ArrayBlockingQueue<>(bufferSize);
      }
    }

    /**
     * Put a record into the buffer, this a blocking call,
     * can blocking indefinitely when the buffer is full.
     * @param record the record to put into buffer
     * @throws InterruptedException
     */
    void put(T record) throws InterruptedException {
      if (useRandomShuffle) {
        // a blocking put
        lock.lock();
        try {
          while (list.size() >= this.bufferSize) {
            notFull.await();
          }
          list.add(record);
          if (list.size() >= pollingThreshold * this.bufferSize) {
            bufferReady.signal();
          }
        } finally {
          lock.unlock();
        }
      } else {
        queue.put(record);
      }
    }

    /**
     * Try to retrieve a record from the buffer. This is
     * a timed blocking call, only block up to given time.
     * If timeout retrieving a record, return null.
     *
     * But for random shuffle read, if the buffer size is
     * not large enough to meet the randomness requirement,
     * this call will be blocking for some time before it
     * even tries to poll data.
     *
     * TODO: currently, given a time and a unit, it may
     * wait for more than that. Due to locking and waiting
     * separately waiting. Need to have a more precise
     * method signature.
     *
     * @param time time to block
     * @param unit unit of time to block
     * @return a record removed from the buffer
     * @throws InterruptedException
     */
    T poll(int time, TimeUnit unit) throws InterruptedException {
      if (useRandomShuffle) {
        boolean lockAcquired = lock.tryLock(time, unit);
        if (!lockAcquired) {
          return null;
        }
        // lock is acquired
        try {
          int attempt = 100;
          while (list.size() < pollingThreshold * this.bufferSize
              && !fetcher.readFinished && attempt-- > 0) {
            bufferReady.await(10, TimeUnit.MILLISECONDS);
          }
          if (attempt <= 0) {
            return null;
          }
          if (list.isEmpty()) {
            return null;
          }
          int index = ran.nextInt(list.size());
          T ret = list.remove(index);
          notFull.signal();
          return ret;
        } finally {
          lock.unlock();
        }
      } else {
        return queue.poll(time, unit);
      }
    }

    boolean isEmpty() {
      if (useRandomShuffle) {
        return list.isEmpty();
      } else {
        return queue.isEmpty();
      }
    }
  }
}
