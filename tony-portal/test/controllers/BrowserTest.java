package controllers;

import com.google.common.collect.ImmutableMap;
import com.linkedin.tony.TonyConfigurationKeys;
import org.junit.Test;
import play.Application;
import play.test.Helpers;
import play.test.TestBrowser;
import play.test.WithBrowser;

import static org.junit.Assert.assertTrue;
import static play.test.Helpers.fakeApplication;


public class BrowserTest extends WithBrowser {

  protected Application provideApplication() {
    return fakeApplication(ImmutableMap.of(
        TonyConfigurationKeys.TONY_HISTORY_LOCATION, "/dummy/",
        TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE, "/dummy/intermediate",
        TonyConfigurationKeys.TONY_HISTORY_FINISHED, "/dummy/finished")
    );
  }

  protected TestBrowser provideBrowser(int port) {
    return Helpers.testBrowser(port);
  }

  @Test
  public void test() {
    browser.goTo("http://localhost:" + play.api.test.Helpers.testServerPort());
    assertTrue(browser.pageSource().contains("TonY Portal"));
  }
}
