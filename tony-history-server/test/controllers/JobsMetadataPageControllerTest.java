package controllers;

import org.testng.Assert;
import org.testng.annotations.Test;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;

import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.route;


public class JobsMetadataPageControllerTest extends WithApplication {

  @Override
  protected Application provideApplication() {
    return new GuiceApplicationBuilder().configure("tony.historyFolder", "/dummy/").build();
  }

  @Test
  public void testIndex() {
    Http.RequestBuilder request = new Http.RequestBuilder().method(GET).uri("/");

    Result result = route(app, request);
    Assert.assertEquals(OK, result.status());
  }
}
