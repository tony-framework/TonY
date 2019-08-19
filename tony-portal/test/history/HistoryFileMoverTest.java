package history;

import cache.CacheWrapper;
import com.google.common.io.Files;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.util.ParserUtils;
import com.typesafe.config.Config;
import hadoop.Requirements;
import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class HistoryFileMoverTest {
  @Mock
  Config config;

  @Mock
  Requirements reqs;

  @Mock
  CacheWrapper cacheWrapper;

  private static File tempDir;
  private static File intermediateDir;
  private static File finishedDir;

  @BeforeClass
  public static void setup() {
    tempDir = Files.createTempDir();
    intermediateDir = new File(tempDir, "intermediate");
    intermediateDir.mkdirs();
    finishedDir = new File(tempDir, "finished");
    finishedDir.mkdirs();
  }

  @Test
  public void testMoveIntermediateToFinished() throws IOException, InterruptedException {
    // Add a completed application in the intermediate dir
    String appId = "application_123_456";
    File appDir = new File(intermediateDir, appId);
    appDir.mkdirs();
    long endTime = System.currentTimeMillis();
    File events = new File(appDir, appId + "-123-" + endTime + "-user1-SUCCEEDED." + Constants.HISTFILE_SUFFIX);
    events.createNewFile();

    // Make sure year/month/day directories created in finished directory are based on finished time set in
    // jhist file name and NOT based off the application directory's modification time.
    if (!appDir.setLastModified(0)) {
      throw new RuntimeException();
    }

    when(config.hasPath(TonyConfigurationKeys.TONY_HISTORY_MOVER_INTERVAL_MS)).thenReturn(false);
    when(config.getString(TonyConfigurationKeys.TONY_HISTORY_FINISHED_DIR_TIMEZONE)).thenReturn("UTC");
    when(reqs.getFileSystem()).thenReturn(FileSystem.getLocal(new Configuration()));
    when(reqs.getIntermediateDir()).thenReturn(new Path(intermediateDir.getAbsolutePath()));
    when(reqs.getFinishedDir()).thenReturn(new Path(finishedDir.getAbsolutePath()));

    // start mover
    new HistoryFileMover(config, reqs, cacheWrapper);
    Thread.sleep(250);

    // verify application directory was moved
    Date endDate = new Date(endTime);
    ZoneId zoneId = ZoneId.of("UTC");
    File finalDir = new File(finishedDir,
        ParserUtils.getYearMonthDayDirectory(endDate, zoneId) + Path.SEPARATOR + appId);
    Assert.assertFalse(appDir.exists());
    Assert.assertTrue(finalDir.isDirectory());
  }
}
