package utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import models.JobConfig;
import models.JobMetadata;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import play.Logger;

import static utils.HdfsUtils.*;


/**
 * The class handles parsing different file format
 */
public class ParserUtils {
  private static final Logger.ALogger LOG = Logger.of(ParserUtils.class);

  @VisibleForTesting
  static boolean isValidMetadata(String[] metadata, String jobIdRegex) {
    if (metadata.length != 5) {
      LOG.error("Missing fields in metadata");
      return false;
    }
    return metadata[0].matches(jobIdRegex) && metadata[1].matches("\\d+") && metadata[2].matches("\\d+")
        && metadata[3].matches(metadata[3].toLowerCase()) && metadata[4].equals(metadata[4].toUpperCase());
  }

  public static JobMetadata parseMetadata(FileSystem fs, Path jobFolderPath, String jobIdRegex) {
    String[] metadata;
    JobMetadata jobMetadata = null;
    if (!pathExists(fs, jobFolderPath)) {
      return null;
    }

    try {
      metadata = Arrays.stream(fs.listStatus(jobFolderPath))
          .filter(f -> f.getPath().toString().contains("jhist")) // ignore config.xml
          .map(histFilePath -> HdfsUtils.getJobId(histFilePath.toString()))
          .map(histFile -> histFile.substring(0, histFile.lastIndexOf('.'))) // remove .jhist
          .map(histFileNoExt -> histFileNoExt.split("-"))
          .flatMap(Arrays::stream)
          .toArray(String[]::new);
    } catch (IOException e) {
      LOG.error("Failed to scan " + jobFolderPath.toString(), e);
      return null;
    }

    if (!isValidMetadata(metadata, jobIdRegex)) {
      // this should never happen unless user rename the history file
      LOG.error("Metadata isn't valid");
      return null;
    }

    LOG.debug("Successfully parsed metadata");
    return JobMetadata.newInstance(metadata);
  }

  public static List<JobConfig> parseConfig(FileSystem fs, Path configFilePath) {
    if (!pathExists(fs, configFilePath)) {
      return new ArrayList<>();
    }
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    List<JobConfig> configs = new ArrayList<>();

    try (FSDataInputStream inStrm = fs.open(configFilePath)) {
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(inStrm);
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
    } catch (SAXException e) {
      LOG.error("Failed to parse config file", e);
      return new ArrayList<>();
    } catch (ParserConfigurationException e) {
      LOG.error("Failed to init XML parser", e);
      return new ArrayList<>();
    } catch (IOException e) {
      LOG.error("Failed to read config file", e);
      return new ArrayList<>();
    }
    LOG.debug("Successfully parsed config");
    return configs;
  }

  private ParserUtils() { }
}
