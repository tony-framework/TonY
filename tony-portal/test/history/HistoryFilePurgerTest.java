package history;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;


public class HistoryFilePurgerTest {
  @Test
  public void testPurgeFinished() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    File finishedDir = setupFinishedDir();
    LocalDate cutOffDate = LocalDate.of(2019, 4, 28);

    HistoryFilePurger.purgeFinishedDir(localFs, new Path(finishedDir.getPath()), cutOffDate);

    // verify cleaning
    Assert.assertFalse(new File(finishedDir, "2018").exists());
    Assert.assertFalse(new File(finishedDir, "2019/01").exists());
    Assert.assertFalse(new File(finishedDir, "2019/02").exists());
    Assert.assertFalse(new File(finishedDir, "2019/03").exists());
    Assert.assertEquals(3, new File(finishedDir, "2019/04").listFiles().length);
    Assert.assertTrue(new File(finishedDir, "2019/04/28").exists());
    Assert.assertTrue(new File(finishedDir, "2019/04/29").exists());
    Assert.assertTrue(new File(finishedDir, "2019/04/30").exists());
  }

  @Test
  public void testPurgeIntermediate() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    File intermediateDir = setupIntermediateDir();
    LocalDate cutOffDate = LocalDate.of(2019, 4, 28);

    HistoryFilePurger.purgeIntermediateDir(localFs, new Path(intermediateDir.getPath()), cutOffDate);

    Assert.assertEquals(2, intermediateDir.listFiles().length);
    Assert.assertTrue(new File(intermediateDir, "application_123_2").exists());
    Assert.assertTrue(new File(intermediateDir, "application_123_3").exists());
  }

  private File setupFinishedDir() {
    File tempDir = Files.createTempDir();
    File finishedDir = new File(tempDir, "finished");
    finishedDir.mkdir();

    new File(finishedDir, "2018").mkdir();

    File dir2019 = new File(finishedDir, "2019");
    dir2019.mkdir();
    new File(dir2019, "01").mkdir();
    new File(dir2019, "02").mkdir();
    new File(dir2019, "03").mkdir();
    File dir201904 = new File(dir2019, "04");
    dir201904.mkdir();
    for (int i = 1; i <= 30; i++) {
      new File(dir201904, String.format("%02d", i)).mkdir();
    }

    return finishedDir;
  }

  private File setupIntermediateDir() {
    File tempDir = Files.createTempDir();
    File intermediateDir = new File(tempDir, "intermediate");
    intermediateDir.mkdir();

    File appDir1 = new File(intermediateDir, "application_123_1");
    appDir1.mkdir();
    appDir1.setLastModified(new DateTime(2019, 4, 27, 0, 0).getMillis());

    File appDir2 = new File(intermediateDir, "application_123_2");
    appDir2.mkdir();
    appDir2.setLastModified(new DateTime(2019, 4, 28, 0, 0).getMillis());

    File appDir3 = new File(intermediateDir, "application_123_3");
    appDir3.mkdir();
    appDir3.setLastModified(new DateTime(2019, 4, 29, 0, 0).getMillis());

    return intermediateDir;
  }
}
