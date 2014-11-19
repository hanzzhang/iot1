package com.microsoft.eventhubs.spout;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class EventHubSpoutConfig
  implements Serializable
{
  private static final long serialVersionUID = 1L;
  private final String connectionString;
  private final String namespace;
  private final String entityPath;
  private final String zkConnectionString;
  private final int partitionCount;
  private final int checkpointIntervalInSeconds;
  private final int receiverCredits;
  private String topologyName;
  private IEventDataScheme scheme;
  
  public EventHubSpoutConfig(String username, String password, String namespace, String entityPath, int partitionCount, String zkConnectionString)
  {
    this(username, password, namespace, entityPath, partitionCount, zkConnectionString, 10, 1000);
  }
  
  public EventHubSpoutConfig(String username, String password, String namespace, String entityPath, int partitionCount, String zkConnectionString, int checkpointIntervalInSeconds, int receiverCredits)
  {
    this.connectionString = buildConnectionString(username, password, namespace);
	System.out.println("  connectionString: " + connectionString);   
    this.namespace = namespace;
    this.entityPath = entityPath;
    this.partitionCount = partitionCount;
    this.zkConnectionString = zkConnectionString;
    this.checkpointIntervalInSeconds = checkpointIntervalInSeconds;
    this.receiverCredits = receiverCredits;
	System.out.println("  this.scheme = new EventDataScheme() ");   
    this.scheme = new EventDataScheme();
  }
  
  public String getConnectionString()
  {
    return this.connectionString;
  }
  
  public String getNamespace()
  {
    return this.namespace;
  }
  
  public String getEntityPath()
  {
    return this.entityPath;
  }
  
  public String getZkConnectionString()
  {
    return this.zkConnectionString;
  }
  
  public int getCheckpointIntervalInSeconds()
  {
    return this.checkpointIntervalInSeconds;
  }
  
  public int getPartitionCount()
  {
    return this.partitionCount;
  }
  
  public int getReceiverCredits()
  {
    return this.receiverCredits;
  }
  
  public String getTopologyName()
  {
    return this.topologyName;
  }
  
  public void setTopologyName(String value)
  {
    this.topologyName = value;
  }
  
  public IEventDataScheme getEventDataScheme()
  {
    return this.scheme;
  }
  
  public void setEventDataScheme(IEventDataScheme scheme)
  {
    this.scheme = scheme;
  }
  
  public List<String> getPartitionList()
  {
    List<String> partitionList = new ArrayList<String>();
    for (int i = 0; i < this.partitionCount; i++) {
      partitionList.add(Integer.toString(i));
    }
    return partitionList;
  }
  
  public static String buildConnectionString(String username, String password, String namespace)
  {
    return "amqps://" + username + ":" + encodeString(password) + "@" + namespace + ".servicebus.windows.net";
  }
  
  private static String encodeString(String input)
  {
    try
    {
      return URLEncoder.encode(input, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {}
    return "";
  }
}
