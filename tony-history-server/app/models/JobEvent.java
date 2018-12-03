package models;

import com.linkedin.tony.events.EventType;
import java.util.Date;


public class JobEvent {
  private EventType type;
  private Object event;
  private long timestamp;

  public EventType getType() {
    return type;
  }

  public Object getEvent() {
    return event;
  }

  public Date getTimestamp() {
    return new Date(timestamp);
  }

  public void setType(EventType type) {
    this.type = type;
  }

  public void setEvent(Object event) {
    this.event = event;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
