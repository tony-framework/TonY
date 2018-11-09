package models;

public class JobConfig {
  private String name;
  private String value;
  private boolean isFinal;
  private String source;

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public boolean isFinal() {
    return isFinal;
  }

  public String getSource() {
    return source;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setFinal(boolean aFinal) {
    isFinal = aFinal;
  }

  public void setSource(String source) {
    this.source = source;
  }
}
