package controllers;

import com.linkedin.tony.TonyConfigurationKeys;
import org.junit.Test;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;

import static org.junit.Assert.assertEquals;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.route;


public class JobsMetadataPageControllerTest extends WithApplication {

  @Override
  protected Application provideApplication() {
    Application fakeApp =
        new GuiceApplicationBuilder().configure(TonyConfigurationKeys.TONY_HISTORY_LOCATION, "/dummy/")
            .configure(TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE, "/dummy/intermediate")
            .configure(TonyConfigurationKeys.TONY_HISTORY_FINISHED, "/dummy/finished")
            .build();
    return fakeApp;
  }

  @Test
  public void testIndex() {
    Http.RequestBuilder request = new Http.RequestBuilder().method(GET).uri("/");

    Result result = route(app, request);
    assertEquals(OK, result.status());
  }
}
