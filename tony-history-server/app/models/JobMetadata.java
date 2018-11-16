package models;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JobMetadata {
  private String id;
  private String jobLink;
  private String configLink;
  private String started;
  private String completed;
  private String status;
  private String user;

  public JobMetadata(String id, String jobLink, String configLink, String started, String completed, String status, String user) {
    this.id = id;
    this.jobLink = jobLink;
    this.configLink = configLink;
    this.started = started;
    this.completed = completed;
    this.status = status;
    this.user = user;
  }

  public static JobMetadata newInstance(String[] metadata) {
    DateFormat simple = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
    String id = metadata[0];
    String jobLink = "/jobs/" + id;
    String configLink = "/config/" + id;
    String started = simple.format(new Date(Long.parseLong(metadata[1])));
    String completed = simple.format(new Date(Long.parseLong(metadata[2])));
    String user = metadata[3];
    String status = metadata[4];
    return new JobMetadata(id, jobLink, configLink, started, completed, status, user);
  }

  public String getId() {
    return id;
  }

  public String getJobLink() {
    return jobLink;
  }

  public String getConfigLink() {
    return configLink;
  }

  public String getStarted() {
    return started;
  }

  public String getCompleted() {
    return completed;
  }

  public String getStatus() {
    return status;
  }

  public String getUser() {
    return user;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setJobLink(String jobLink) {
    this.jobLink = jobLink;
  }

  public void setConfigLink(String configLink) {
    this.configLink = configLink;
  }

  public void setStarted(String started) {
    this.started = started;
  }

  public void setCompleted(String completed) {
    this.completed = completed;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setUser(String user) {
    this.user = user;
  }
}
