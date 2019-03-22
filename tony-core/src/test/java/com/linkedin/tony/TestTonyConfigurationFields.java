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
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getMemoryKey(Constants.PS_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getVCoresKey(Constants.PS_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourcesKey(Constants.PS_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getMemoryKey(Constants.WORKER_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getVCoresKey(Constants.WORKER_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getGPUsKey(Constants.WORKER_JOB_NAME));
    xmlPropsToSkipCompare.add(TonyConfigurationKeys.getResourcesKey(Constants.WORKER_JOB_NAME));
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.DOCKER_IMAGE);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_VERSION);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_REVISION);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_BRANCH);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_USER);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_DATE);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_URL);
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.TONY_VERSION_INFO_CHECKSUM);

    // '.' in history.com makes config comparing algorithm think it is a config key, so we ignore it here explicitly
    // this is fixed in 2.9+
    configurationPropsToSkipCompare.add(TonyConfigurationKeys.DEFAULT_TONY_HISTORY_HOST);
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

  /* Base method not available in Hadoop 2.7
  @Test
  public void testXmlAgainstDefaultValuesInConfigurationClass() {
    super.testXmlAgainstDefaultValuesInConfigurationClass();
  } */
}
