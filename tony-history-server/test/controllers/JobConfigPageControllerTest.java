package controllers;

import org.testng.annotations.Test;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

import static org.testng.Assert.*;
import static play.mvc.Http.Status.*;


public class JobConfigPageControllerTest extends WithApplication {

  @Override
  protected Application provideApplication() {
    return new GuiceApplicationBuilder().build();
  }

  @Test
  public void testIndex() {
    // TODO: Write tests
    assertEquals(OK, OK);
  }
}
