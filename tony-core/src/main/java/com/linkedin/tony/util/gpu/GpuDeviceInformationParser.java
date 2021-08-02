/**
 * Modifications to this file are licensed under BSD-2-Clause
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.tony.util.gpu;

import java.io.StringReader;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;


/**
 * Parse XML and get GPU device information
 * Ported from Hadoop 2.9.0
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuDeviceInformationParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      GpuDeviceInformationParser.class);

  private Unmarshaller unmarshaller = null;
  private XMLReader xmlReader = null;

  private void init()
      throws SAXException, ParserConfigurationException, JAXBException {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    // Disable external-dtd since by default nvidia-smi output contains
    // <!DOCTYPE nvidia_smi_log SYSTEM "nvsmi_device_v8.dtd"> in header
    spf.setFeature(
        "http://apache.org/xml/features/nonvalidating/load-external-dtd",
        false);
    spf.setFeature("http://xml.org/sax/features/validation", false);

    JAXBContext jaxbContext = JAXBContext.newInstance(
        GpuDeviceInformation.class);

    this.xmlReader = spf.newSAXParser().getXMLReader();
    this.unmarshaller = jaxbContext.createUnmarshaller();
  }

  public synchronized GpuDeviceInformation parseXml(String xmlContent)
      throws GpuInfoException {
    if (unmarshaller == null) {
      try {
        init();
      } catch (SAXException | ParserConfigurationException | JAXBException e) {
        LOG.error("Exception while initialize parser", e);
        throw new GpuInfoException(e);
      }
    }

    InputSource inputSource = new InputSource(new StringReader(xmlContent));
    SAXSource source = new SAXSource(xmlReader, inputSource);
    try {
      return (GpuDeviceInformation) unmarshaller.unmarshal(source);
    } catch (JAXBException e) {
      LOG.error("Exception while parsing xml", e);
      throw new GpuInfoException(e);
    }
  }
}
