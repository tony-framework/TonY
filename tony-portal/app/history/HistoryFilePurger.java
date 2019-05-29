package history;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.TonyConfigurationKeys;
import com.typesafe.config.Config;
import hadoop.Requirements;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.Logger;
import utils.ConfigUtils;


@Singleton
public class HistoryFilePurger {
  private static final Logger.ALogger LOG = Logger.of(HistoryFilePurger.class);

  @Inject
  public HistoryFilePurger(Config appConf, Requirements requirements) {
    FileSystem fs = requirements.getFileSystem();
    Path intermediateDir = requirements.getIntermediateDir();
    Path finishedDir = requirements.getFinishedDir();
    long retentionSec = ConfigUtils.fetchIntConfigIfExists(appConf,
        TonyConfigurationKeys.TONY_HISTORY_RETENTION_SECONDS,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_RETENTION_SECONDS);

    ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(1);
    long purgerIntervalMs = ConfigUtils.fetchIntConfigIfExists(appConf,
        TonyConfigurationKeys.TONY_HISTORY_PURGER_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_PURGER_INTERVAL_MS);
    String finishedDirTimeZone = ConfigUtils.fetchConfigIfExists(appConf,
        TonyConfigurationKeys.TONY_HISTORY_FINISHED_DIR_TIMEZONE,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_FINISHED_DIR_TIMEZONE);
    ZoneId zoneId = ZoneId.of(finishedDirTimeZone);

    LOG.info("Retention period is " + retentionSec + " seconds");
    LOG.info("Starting background history file purger thread, will run every " + purgerIntervalMs + " milliseconds.");
    scheduledThreadPool.scheduleAtFixedRate(() -> {
      LocalDate cutOffDate = LocalDateTime.now(zoneId).minusSeconds(retentionSec).toLocalDate();
      LOG.info("Purging all history files older than " + cutOffDate);
      try {
        purgeFinishedDir(fs, finishedDir, cutOffDate);
        purgeIntermediateDir(fs, intermediateDir, cutOffDate);
      } catch (Exception e) {
        LOG.error("Encountered exception while purging history directories", e);
      }
    }, 0, purgerIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Deletes all year/month/day directories in the finished dir prior to the cutoff date.
   */
  @VisibleForTesting
  static void purgeFinishedDir(FileSystem fs, Path finishedDir, LocalDate cutOffDate) throws IOException {
    FileStatus[] yearDirs = fs.listStatus(finishedDir, path -> path.getName().matches("\\d{4}"));
    for (FileStatus yearDir : yearDirs) {
      int year = Integer.parseInt(yearDir.getPath().getName());
      LocalDate pathDate = LocalDate.ofYearDay(year, 1).with(TemporalAdjusters.lastDayOfYear());
      if (pathDate.isBefore(cutOffDate)) {
        fs.delete(yearDir.getPath(), true);
        continue;
      }

      FileStatus[] monthDirs = fs.listStatus(yearDir.getPath(), path -> path.getName().matches("\\d{2}"));
      for (FileStatus monthDir : monthDirs) {
        int month = Integer.parseInt(monthDir.getPath().getName());
        pathDate = LocalDate.of(year, month, 1).with(TemporalAdjusters.lastDayOfMonth());
        if (pathDate.isBefore(cutOffDate)) {
          fs.delete(monthDir.getPath(), true);
          continue;
        }

        FileStatus[] dayDirs = fs.listStatus(monthDir.getPath(), path -> path.getName().matches("\\d{2}"));
        for (FileStatus dayDir : dayDirs) {
          int day = Integer.parseInt(dayDir.getPath().getName());
          pathDate = LocalDate.of(year, month, day);
          if (pathDate.isBefore(cutOffDate)) {
            fs.delete(dayDir.getPath(), true);
            continue;
          }
        }
      }
    }
  }

  /**
   * Delete all jobs in the intermediate dir that started before the cut-off date.
   */
  @VisibleForTesting
  static void purgeIntermediateDir(FileSystem fs, Path intermediateDir, LocalDate cutOffDate)
      throws IOException {
    FileStatus[] jobDirs = fs.listStatus(intermediateDir);
    for (FileStatus jobDir : jobDirs) {
      LocalDate jobStartDate =
          Instant.ofEpochMilli(jobDir.getModificationTime()).atZone(ZoneId.systemDefault()).toLocalDate();
      if (jobStartDate.isBefore(cutOffDate)) {
        fs.delete(jobDir.getPath(), true);
        continue;
      }
    }
  }
}
