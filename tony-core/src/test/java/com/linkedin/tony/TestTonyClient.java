/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.util.HashMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestTonyClient {

  @Test
  public void testTonyClientInit() throws Exception {
    String[] args = { "-conf", TonyConfigurationKeys.AM_GPUS + "=1" };
    TonyClient client = new TonyClient();
    client.init(args);
    assertEquals(1, client.getTonyConf().getInt(TonyConfigurationKeys.AM_GPUS, TonyConfigurationKeys.DEFAULT_AM_GPUS));
  }

//  @Test
//  public void testBuildCommand() {
//    String command = TonyClient.buildCommand(1000, null, "venv/python",
//        "/user/pi/myvenv.zip",
//        "ls", "/user/pi", new HashMap<>(), new HashMap<>());
//    String expected = "{{JAVA_HOME}}/bin/java -Xmx800m -Dyarn.app.container.log.dir=<LOG_DIR> com.linkedin.tony."
//        + "TonyApplicationMaster --python_binary_path venv/python --python_venv myvenv.zip --executes ls --hdfs_"
//        + "classpath /user/pi 1><LOG_DIR>/amstdout.log 2><LOG_DIR>/amstderr.log";
//    assertEquals(command, expected);
//  }
}
