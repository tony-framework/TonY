/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestLocalizableResource {
  @Test
  public void testLocalResourceParsing() throws IOException, ParseException {
    FileSystem fs = FileSystem.get(new Configuration());
    LocalizableResource resource = new LocalizableResource("tony-core/src/test/resources/test.zip");
    resource.parse(fs);
    Assert.assertNotNull(resource.toLocalResource().getResource());
    Assert.assertEquals(resource.getLocalFileName(), "test.zip");

    LocalizableResource resource2 = new LocalizableResource("tony-core/src/test/resources/test.zip::ok.123");
    resource2.parse(fs);
    Assert.assertNotNull(resource2.toLocalResource().getResource());
    Assert.assertEquals(resource2.getLocalFileName(), "ok.123");

    LocalizableResource resource3 = new LocalizableResource("tony-core/src/test/resources/test.zip::ok#archive");
    resource3.parse(fs);
    Assert.assertNotNull(resource3.toLocalResource().getResource());
    Assert.assertSame(resource3.toLocalResource().getType(), LocalResourceType.ARCHIVE);
    Assert.assertEquals(resource3.getLocalFileName(), "ok");

    LocalizableResource resource4 = new LocalizableResource("tony-core/src/test/resources/test.zip#archive");
    resource4.parse(fs);
    Assert.assertNotNull(resource4.toLocalResource().getResource());
    Assert.assertSame(resource4.toLocalResource().getType(), LocalResourceType.ARCHIVE);
    Assert.assertEquals(resource4.getLocalFileName(), "test.zip");
  }

}
