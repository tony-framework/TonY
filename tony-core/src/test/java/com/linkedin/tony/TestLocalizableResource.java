package com.linkedin.tony;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestLocalizableResource {

    @Test
    public void testLocalResourceParsing() throws IOException, ParseException {
        FileSystem fs = FileSystem.get(new Configuration());
        LocalizableResource resource = new LocalizableResource("tony-core/src/test/resources/test.zip");
        resource.parse(fs);
        Assert.assertNotNull(resource.getLocalResource().getResource());
        Assert.assertEquals(resource.getLocalFileName(), "test.zip");


        LocalizableResource resource2 = new LocalizableResource("tony-core/src/test/resources/test.zip::ok.123");
        resource2.parse(fs);
        Assert.assertNotNull(resource2.getLocalResource().getResource());
        Assert.assertEquals(resource2.getLocalFileName(), "ok.123");

        LocalizableResource resource3 = new LocalizableResource("tony-core/src/test/resources/test.zip::ok#archive");
        resource3.parse(fs);
        Assert.assertNotNull(resource3.getLocalResource().getResource());
        Assert.assertSame(resource3.getLocalResource().getType(), LocalResourceType.ARCHIVE);
    }

}
