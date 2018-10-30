package models;

public class JobMetadata {
  private String id;
  private String jobLink;
  private String configLink;
  private String started;
  private String completed;

  public String getId() {
    return id;
  }

  public String getJobLink() {
    return jobLink;
  }

  public String getConfigLink() {
    return configLink;
  }

  public String getCompleted() {
    return completed;
  }

  public String getStarted() {
    return started;
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

}
