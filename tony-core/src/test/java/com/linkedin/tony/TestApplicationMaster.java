/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestApplicationMaster {
  @Test
  public void testBuildBaseTaskCommand() {
    // null venv zip
    String actual = TonyClient.buildTaskCommand(null, "/export/apps/python/2.7/bin/python2.7",
                                                               "src/main/python/my_awesome_script.py", "--input_dir hdfs://default/foo/bar");
    String expected = "/export/apps/python/2.7/bin/python2.7 "
                      + "src/main/python/my_awesome_script.py --input_dir hdfs://default/foo/bar";
    Assert.assertEquals(actual, expected);

    // venv zip is set, but should be ignored since pythonBinaryPath is absolute
    actual = TonyClient.buildTaskCommand(null, "/export/apps/python/2.7/bin/python2.7",
                                                        "src/main/python/my_awesome_script.py", "--input_dir hdfs://default/foo/bar");
    expected = "/export/apps/python/2.7/bin/python2.7 "
               + "src/main/python/my_awesome_script.py --input_dir hdfs://default/foo/bar";
    Assert.assertEquals(actual, expected);

    // pythonBinaryPath is relative, so should be appended to "venv"
    actual = TonyClient.buildTaskCommand(null, "Python/bin/python",
                                                        "src/main/python/my_awesome_script.py", "--input_dir hdfs://default/foo/bar");
    expected = "Python/bin/python "
               + "src/main/python/my_awesome_script.py --input_dir hdfs://default/foo/bar";
    Assert.assertEquals(actual, expected);

    // pythonBinaryPath is relative, so should be appended to "venv"
    actual = TonyClient.buildTaskCommand("hello", "Python/bin/python",
        "src/main/python/my_awesome_script.py", "--input_dir hdfs://default/foo/bar");
    expected = "venv/Python/bin/python "
        + "src/main/python/my_awesome_script.py --input_dir hdfs://default/foo/bar";
    Assert.assertEquals(actual, expected);
  }
}
