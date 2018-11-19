package controllers;

import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;
import play.Application;
import play.test.Helpers;
import play.test.TestBrowser;
import play.test.WithBrowser;

import static play.test.Helpers.*;


public class BrowserTest extends WithBrowser {

  protected Application provideApplication() {
    return fakeApplication(ImmutableMap.of("tony.historyFolder", "/dummy/"));
  }

  protected TestBrowser provideBrowser(int port) {
    return Helpers.testBrowser(port);
  }

  @Test
  public void test() {
    browser.goTo("http://localhost:" + play.api.test.Helpers.testServerPort());
    Assert.assertTrue(browser.pageSource().contains("Tony History Server"));
  }
}
