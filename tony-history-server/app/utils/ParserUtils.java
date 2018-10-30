package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import models.JobConfig;
import models.JobMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import play.Logger;
import play.libs.Json;

import static utils.HdfsUtils.*;

/**
 * The class handles parsing different file format
 */
public class ParserUtils {
  private static final Logger.ALogger LOG = Logger.of(ParserUtils.class);

  public static JobMetadata parseMetadata(FileSystem fs, Path metricsFilePath) {
    if (!isPathValid(fs, metricsFilePath)) {
      return new JobMetadata();
    }

    String fileContent = contentOfHdfsFile(fs, metricsFilePath);
    JobMetadata jobMetadata = new JobMetadata();
    JsonNode jObj;
    try {
      jObj = Json.parse(fileContent);
    } catch (JsonSyntaxException e) {
      LOG.error("Couldn't parse metadata", e);
      return jobMetadata;
    }
    jobMetadata.setId(jObj.get("id").textValue());
    jobMetadata.setJobLink(jObj.get("url").textValue());
    jobMetadata.setConfigLink("/jobs/" + jobMetadata.getId());
    jobMetadata.setStarted(jObj.get("started").textValue());
    jobMetadata.setCompleted(jObj.get("completed").textValue());
    LOG.info("Successfully parsed metadata");
    return jobMetadata;
  }

  public static List<JobConfig> parseConfig(FileSystem fs, Path configFilePath) {
    if (!isPathValid(fs, configFilePath)) {
      return new ArrayList<>();
    }

    File configFile = new File(copyFromHdfs(fs, configFilePath));
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    List<JobConfig> configs = new ArrayList<>();

    try {
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(configFile);
      doc.getDocumentElement().normalize();

      NodeList properties = doc.getElementsByTagName("property");

      for (int i = 0; i < properties.getLength(); i++) {
        Node property = properties.item(i);
        if (property.getNodeType() == Node.ELEMENT_NODE) {
          Element p = (Element) property;
          JobConfig jobConf = new JobConfig();
          jobConf.setName(p.getElementsByTagName("name").item(0).getTextContent());
          jobConf.setValue(p.getElementsByTagName("value").item(0).getTextContent());
          jobConf.setFinal(p.getElementsByTagName("final").item(0).getTextContent().equals("true"));
          jobConf.setSource(p.getElementsByTagName("source").item(0).getTextContent());
          configs.add(jobConf);
        }
      }
    } catch (Exception e) {
      LOG.error("Couldn't parse config", e);
    } finally {
      configFile.delete();
    }
    LOG.info("Successfully parsed config");
    return configs;
  }
}
