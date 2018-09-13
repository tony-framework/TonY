/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTonyConfigurationFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String(Constants.TONY_DEFAULT_XML);
    configurationClasses = new Class[] { TonyConfigurationKeys.class };

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;
  }

  @BeforeTest
  public void setupTestConfigurationFields() throws Exception {
    super.setupTestConfigurationFields();
  }

  @Test
  public void testCompareConfigurationClassAgainstXml() {
    super.testCompareConfigurationClassAgainstXml();
  }

  @Test
  public void testCompareXmlAgainstConfigurationClass() {
    super.testCompareXmlAgainstConfigurationClass();
  }

  @Test
  public void testXmlAgainstDefaultValuesInConfigurationClass() {
    super.testXmlAgainstDefaultValuesInConfigurationClass();
  }
}
