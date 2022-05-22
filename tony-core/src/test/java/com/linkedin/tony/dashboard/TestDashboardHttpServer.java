/*
 * Copyright 2022 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.tony.dashboard;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonySession;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestDashboardHttpServer {

    @Test
    public void testServer() throws Exception {
        Configuration tonyConf = new Configuration(false);
        TonySession session = new TonySession.Builder().setTonyConf(tonyConf).build();

        TonySession.TonyTask worker0 =
                session.buildTonyTask(Constants.WORKER_JOB_NAME, "0", "localhost");
        TonySession.TonyTask worker1 =
                session.buildTonyTask(Constants.WORKER_JOB_NAME, "1", "localhost");
        TonySession.TonyTask worker2 =
                session.buildTonyTask(Constants.WORKER_JOB_NAME, "2", "localhost");

        worker0.setTaskInfo();
        worker1.setTaskInfo();
        worker2.setTaskInfo();

        session.addTask(worker0);
        session.addTask(worker1);
        session.addTask(worker2);

        DashboardHttpServer server = DashboardHttpServer.builder()
                .runtimeType("tensorflow")
                .amHostName("localhost")
                .amLogUrl("localhost:xxxxx")
                .session(session)
                .build();
        server.start();

        AssertJUnit.assertTrue(server.isStarted());

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpget = new HttpGet("http://localhost:" + server.getServerPort() + "/");
        CloseableHttpResponse response = httpclient.execute(httpget);
        AssertJUnit.assertEquals(200, response.getStatusLine().getStatusCode());

        AssertJUnit.assertNotNull(
                IOUtils.toString(response.getEntity().getContent())
        );
    }
}
