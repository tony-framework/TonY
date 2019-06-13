/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTonyConfigurationFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = Constants.TONY_DEFAULT_XML;
    configurationClasses = new Class[] { TonyConfigurationKeys.class };

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;

    xmlPropsToSkipCompare = xmlPropsToSkipCompare == null ? new HashSet<>() : xmlPropsToSkipCompare;
    configurationPropsToSkipCompare = configurationPropsToSkipCompare == null ? new HashSet<>() : configurationPropsToSkipCompare;

    // We don't explicitly declare constants for these, since the configured TensorFlow job names
    // are determined at runtime. But we still need default values for them in tony-default.xml.
    // So ignore the fact that they exist in tony-default.xml and not in TonyConfigurationKeys.
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME, Constants.MEMORY));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME, Constants.VCORES));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourcesKey(Constants.PS_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.MEMORY));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.VCORES));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.GPUS));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourcesKey(Constants.WORKER_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getMaxTotalResourceKey(Constants.GPUS));
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_VERSION);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_REVISION);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_BRANCH);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_USER);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_DATE);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_URL);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_CHECKSUM);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.EXECUTION_ENV);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.GPU_PATH_TO_EXEC);
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
}
