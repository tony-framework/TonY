package models;

import java.util.Date;

public class JobMetadata {
  private String id;
  private String jobLink;
  private String configLink;
  private long started;
  private long completed;
  private String status;
  private String user;

  public JobMetadata(String id, String jobLink, String configLink, long started, long completed, String status, String user) {
    this.id = id;
    this.jobLink = jobLink;
    this.configLink = configLink;
    this.started = started;
    this.completed = completed;
    this.status = status;
    this.user = user;
  }

  public static JobMetadata newInstance(String histFileName) {
    String histFileNoExt = histFileName.substring(0, histFileName.lastIndexOf('.'));
    String[] metadata = histFileNoExt.split("-");
    String id = metadata[0];
    String jobLink = "/jobs/" + id;
    String configLink = "/config/" + id;
    long started = Long.parseLong(metadata[1]);
    long completed = Long.parseLong(metadata[2]);
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

  public Date getStartedDate() {
    return new Date(started);
  }

  public Date getCompletedDate() {
    return new Date(completed);
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

  public void setUser(String user) {
    this.user = user;
  }
}
