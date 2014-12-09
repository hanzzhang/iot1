Store Event Hub Messages to Windows Azure Blob with Trident
===========================================================
Prerequisites
•	An Azure subscription
•	Visual Studio with the Microsoft Azure SDK for .NET
•	Java and JDK
•	Maven
•	Git
Configure Event Hub
•	Create Event Hub.
•	Once the event hub has been created, select the namespace, then select Event Hubs. Finally, select the event hub you created.
•	Select Configure, then create two new access policies using the following information.
NAME		PERMISSIONS
Devices		Send
Storm		Listen
Send messages to Event Hub
Create a .NET console application to generate events for 10 devices every second, until you stop the application by pressing a key.
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Microsoft.ServiceBus;
using System.Threading;
using System.Runtime.Serialization;

namespace SendEvents
{
    class Program
    {
        static int numberOfDevices = 1000;
        static string eventHubName = "hanzeventhub1";
        static string eventHubNamespace = "hanzeventhub1-ns";
        static string sharedAccessPolicyName = "devices";
        static string sharedAccessPolicyKey = "Lc4CQbXsZTtkyDOrj5cGB0t0zkLLcMjAT/+/40993FA=";
        static void Main(string[] args)
        {
            var settings = new MessagingFactorySettings()
            {
                TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(sharedAccessPolicyName, sharedAccessPolicyKey),
                TransportType = TransportType.Amqp
            };
            var factory = MessagingFactory.Create(ServiceBusEnvironment.CreateServiceUri("sb", eventHubNamespace, ""), settings);

            EventHubClient client = EventHubClient.CreateFromConnectionString("Endpoint=sb://hanzeventhub1-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=VremOpcEIYzpxXLIkqjgzT2ZJXBVdTSYxFhkRW6SiY8=", eventHubName);

            try
            {
                List<Task> tasks = new List<Task>();
                Console.WriteLine("Sending messages to Event Hub {0}", client.Path);
                Random random = new Random();
                while (!Console.KeyAvailable)
                {
                    // One event per device
                    for (int devices = 0; devices < numberOfDevices; devices++)
                    {
                        // Create the event
                        Event info = new Event()
                        {
                            lat = -30 + random.Next(75),
                            lng = -120+random.Next(70),
                            time = DateTime.UtcNow.Ticks,
                            diagnosisCode = (310 + random.Next(20)).ToString()
                        };
                        // Serialize to JSON
                        var serializedString = JsonConvert.SerializeObject(info);
                        Console.WriteLine(serializedString);
                        EventData data = new EventData(Encoding.UTF8.GetBytes(serializedString))
                        {
                            PartitionKey = info.diagnosisCode
                        };

                        // Send the message to Event Hub
                        tasks.Add(client.SendAsync(data));
                    }
                    Thread.Sleep(1000);
                };

                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception exp)
            {
                Console.WriteLine("Error on send: " + exp.Message);
            }

        }
    }

    [DataContract]
    public class Event
    {
        [DataMember]
        public double lat { get; set; }
        [DataMember]
        public double lng { get; set; }
        [DataMember]
        public long time { get; set; }
        [DataMember]
        public string diagnosisCode { get; set; }

    }
}
Create the HDInsight Storm cluster
•	Sign in to the azure and create a storm cluster and create a Storm cluster
Create the Azure Redis Cache
•	Create Azure Redis Cache following the following link: How to Use Azure Redis Cache (http://azure.microsoft.com/en-us/documentation/articles/cache-dotnet-how-to-use-azure-redis-cache/ )
Download and build the Event Hub spout
Several of the dependencies used in this project must be downloaded and built individually, then installed into the local Maven repository on your development environment. 
In order to receive data from Event Hub, we will use the eventhubs-storm-spout.
•	Use Remote Desktop to connect to your Storm cluster, then copy the %STORM_HOME%\examples\eventhubspout\eventhubs-storm-spout-0.9-jar-with-dependencies.jar file to your local development environment. 
•	Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.
mvn install:install-file -Dfile=eventhubs-storm-spout-0.9-jar-with-dependencies.jar -DgroupId=com.microsoft.eventhubs -DartifactId=eventhubs-storm-spout -Dversion=0.9 -Dpackaging=jar
Clone and build the Jedis SDK with SSL Support
•	Clone jedis sdk with SSL support from https://github.com/RedisLabs/jedis
•	Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

mvn clean install -Dmaven.test.skip=true
Clone and build the Microsoft Azure SDK for Java
•	Clone Microsoft Azure SDK for Java from https://github.com/Azure/azure-sdk-for-java
•	Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

mvn clean install -Dmaven.test.skip=true
Clone and build Windows Azure Storage libraries for Java
•	Clone Windows Azure Storage libraries for Java (https://github.com/Azure/azure-storage-java)
•	Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

mvn clean install -Dmaven.test.skip=true
Scaffold the Storm topology project
•	Use the following Maven command to create the scaffolding for the Trident topology project.
mvn archetype:generate -DgroupId=com.contoso.app.trident -DartifactId=eventhub-blobwriter -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
Add dependencies in pom.xml
Using a text editor, open the pom.xml file, and add the following to the <dependencies> section. You can add them at the end of the section, after the dependency for junit.
		<dependency>
			<groupId>org.apache.storm</groupId>
			<artifactId>storm-core</artifactId>
			<version>0.9.1-incubating</version>
			<!-- keep storm out of the jar-with-dependencies -->
			<scope>provided</scope>
		</dependency>
		<dependency>
			<groupId>com.google.guava</groupId>
			<artifactId>guava</artifactId>
			<version>13.0.1</version>
		</dependency>
		<dependency>
			<groupId>commons-collections</groupId>
			<artifactId>commons-collections</artifactId>
			<version>3.2.1</version>
		</dependency>
		<dependency>
			<groupId>org.slf4j</groupId>
			<artifactId>slf4j-api</artifactId>
			<version>1.7.7</version>
			<!-- keep out of the jar-with-dependencies -->
			<scope>provided</scope>
		</dependency>
		<dependency>
			<groupId>com.microsoft.eventhubs</groupId>
			<artifactId>eventhubs-storm-spout</artifactId>
			<version>0.9</version>
		</dependency>
		<dependency>
			<groupId>com.google.code.gson</groupId>
			<artifactId>gson</artifactId>
			<version>2.3</version>
		</dependency>
		<dependency>
			<groupId>redis.clients</groupId>
			<artifactId>jedis</artifactId>
			<version>2.5.0.ssl</version>
		</dependency>

		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-storage</artifactId>
			<version>1.3.1</version>
		</dependency>
		
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management-compute</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management-network</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management-sql</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management-storage</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-management-websites</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-media</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-servicebus</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>azure-serviceruntime</artifactId>
			<version>0.6.0</version>
		</dependency>
		<dependency>
			<groupId>com.microsoft.windowsazure.storage</groupId>
			<artifactId>microsoft-windowsazure-storage-sdk</artifactId>
			<version>0.6.0</version>
	</dependency>
Note: Some dependencies are marked with a scope of provided to indicate that these dependencies should be downloaded from the Maven repository and used to build and test the application locally, but that they will also be available in your runtime environment and do not need to be compiled and included in the JAR created by this project.
Add plugins in pom.xml
•	At the end of the pom.xml file, right before the </project> entry, add the following.
	<build>
		<plugins>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-compiler-plugin</artifactId>
				<version>3.2</version>
				<configuration>
					<source>1.7</source>
					<target>1.7</target>
				</configuration>
			</plugin>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-shade-plugin</artifactId>
				<version>2.3</version>
				<configuration>
					<createDependencyReducedPom>true</createDependencyReducedPom>
					<transformers>
						<transformer
							implementation="org.apache.maven.plugins.shade.resource.ApacheLicenseResourceTransformer">
						</transformer>
					</transformers>
				</configuration>
				<executions>
					<execution>
						<phase>package</phase>
						<goals>
							<goal>shade</goal>
						</goals>
						<configuration>
							<transformers>
								<transformer
									implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
								<transformer
									implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
									<mainClass></mainClass>
								</transformer>
							</transformers>
						</configuration>
					</execution>
				</executions>
			</plugin>
			<plugin>
				<groupId>org.codehaus.mojo</groupId>
				<artifactId>exec-maven-plugin</artifactId>
				<version>1.2.1</version>
				<executions>
					<execution>
						<goals>
							<goal>exec</goal>
						</goals>
					</execution>
				</executions>
				<configuration>
					<executable>java</executable>
					<includeProjectDependencies>true</includeProjectDependencies>
					<includePluginDependencies>false</includePluginDependencies>
					<classpathScope>compile</classpathScope>
					<mainClass>${storm.topology}</mainClass>
				</configuration>
			</plugin>
		</plugins>
		<resources>
			<resource>
				<directory>${basedir}/conf</directory>
				<filtering>false</filtering>
				<includes>
					<include>Config.properties</include>
				</includes>
			</resource>
		</resources>
	</build>
•	This tells Maven to do the following when building the project:
o	Include the /conf/Config.properties resource file. This file will be created later, but it will contain configuration information for connecting to Azure Event Hub.
o	Use the maven-compiler-plugin to compile the application.
o	Use the maven-shade-plugin to build an uberjar or fat jar, which contains this project and any required dependencies.
o	Use the exec-maven-plugin, which allows you to run the application locally without a Hadoop cluster.
Add configuration files
eventhubs-storm-spout reads configuration information from a Config.properties file. This tells it what Event Hub to connect to. While you can specify a configuration file when starting the topology on a cluster, including one in the project gives you a known default configuration.
•	In the eventhub-blobwriter directory, create a new directory named conf. This will be a sister directory of src.
•	In the conf directory, create file Config.properties - contains settings for event hub
•	Use the following as the contents for the Config.properties file.
eventhubspout.username = storm
eventhubspout.password = pHwdU08RZhSP2rlN8DtVqW5rgWJFlhWEmP5THeaqYQA=
eventhubspout.namespace = hanzeventhub1-ns
eventhubspout.entitypath = hanzeventhub1
eventhubspout.partitions.count = 10
# if not provided, will use storm's zookeeper settings
# zookeeper.connectionstring=localhost:2181
eventhubspout.checkpoint.interval = 10
eventhub.receiver.credits = 1024
storage.blob.account.name = hanzstorage
storage.blob.account.key = w9TEpvGTusvFlGAdCoWdDrwqLzy6er0Zm5YKdDD0YTkQdOj3WufeVrgd2c8q8amLR0o6xD0tBChcIIA+DCgxXA==
storage.blob.account.container = hanzstorm2
#number of blocks in each blob default to 50000
storage.blob.block.number.max = 50000
#max KB in each block default to 4096KB
storage.blob.block.kb.max = 4096
Add EventHubMessage class to support serialization to and from JSON
•	Create a new file EventHubMessage.jav in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import java.io.Serializable;
public class EventHubMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public double lat;
    public double lng;
    public long time;
    public String id;
    public EventHubMessage (double lat, double lng, long time, String id) {
        super();
        this.time = time;
        this.lat = lat;
        this.lng = lng;
        this.id = id;
    }
}
Add BlobWriter class support uploading data to azure blob
•	Create a new file BlobWriter.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.BlockSearchMode;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.core.Base64;

public class BlobWriter {
	static public void upload(Properties properties, String blobname, String blockIdStr, String data) {
		Logger logger = (Logger) LoggerFactory.getLogger(BlobWriter.class);
		InputStream stream = null;
		try {
			String accountName = properties.getProperty("storage.blob.account.name");
			String accountKey = properties.getProperty("storage.blob.account.key");
			String containerName = properties.getProperty("storage.blob.account.container");

			String connectionStrFormatter = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s";
			String connectionStr = String.format(connectionStrFormatter, accountName, accountKey);

			logger.info("accountName = " + accountName);
			logger.info("accountKey = " + accountKey);
			logger.info("containerName = " + containerName);
			logger.info("connectionStr = " + connectionStr);

			CloudStorageAccount account = CloudStorageAccount.parse(String.format(connectionStr, accountName, accountKey));
			CloudBlobClient _blobClient = account.createCloudBlobClient();
			CloudBlobContainer _container = _blobClient.getContainerReference(containerName);
			_container.createIfNotExists();
			CloudBlockBlob blockBlob = _container.getBlockBlobReference(blobname);
			BlobRequestOptions blobOptions = new BlobRequestOptions();
			ArrayList<BlockEntry> newBlockList = new ArrayList<BlockEntry>();

			stream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
			BlockEntry newBlock = new BlockEntry(Base64.encode(blockIdStr.getBytes()), BlockSearchMode.UNCOMMITTED);
			blockBlob.uploadBlock(newBlock.getId(), stream, -1);
			newBlockList.add(newBlock);

			ArrayList<BlockEntry> blocksBeforeUpload = new ArrayList<BlockEntry>();
			if (blockBlob.exists(AccessCondition.generateEmptyCondition(), blobOptions, null)) {
				blocksBeforeUpload = blockBlob.downloadBlockList(BlockListingFilter.COMMITTED, null, blobOptions, null);
			}
			blocksBeforeUpload.addAll(newBlockList);
			blockBlob.commitBlockList(blocksBeforeUpload);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	static public void remove(Properties properties, String blockIdStrFormat, String blobname, String blockIdStr) {
		// remove blocks with blockid >= blockIdStr
		Logger logger = (Logger) LoggerFactory.getLogger(BlobWriter.class);
		try {
			String accountName = properties.getProperty("storage.blob.account.name");
			String accountKey = properties.getProperty("storage.blob.account.key");
			String containerName = properties.getProperty("storage.blob.account.container");

			String connectionStrFormatter = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s";
			String connectionStr = String.format(connectionStrFormatter, accountName, accountKey);

			logger.info("accountName = " + accountName);
			logger.info("accountKey = " + accountKey);
			logger.info("containerName = " + containerName);
			logger.info("connectionStr = " + connectionStr);

			CloudStorageAccount account = CloudStorageAccount.parse(String.format(connectionStr, accountName, accountKey));
			CloudBlobClient _blobClient = account.createCloudBlobClient();
			CloudBlobContainer _container = _blobClient.getContainerReference(containerName);
			_container.createIfNotExists();
			CloudBlockBlob blockBlob = _container.getBlockBlobReference(blobname);
			BlobRequestOptions blobOptions = new BlobRequestOptions();

			ArrayList<BlockEntry> blocksBeforeUpload = new ArrayList<BlockEntry>();
			if (blockBlob.exists(AccessCondition.generateEmptyCondition(), blobOptions, null)) {
				blocksBeforeUpload = blockBlob.downloadBlockList(BlockListingFilter.COMMITTED, null, blobOptions, null);
			}
			int blockid = Integer.parseInt(blockIdStr);
			int size = blocksBeforeUpload.size();
			//int size = 50000;
			for (int i = size; i >= blockid; i--) {
				String idStr = String.format(blockIdStrFormat, i);
				BlockEntry entry = new BlockEntry(Base64.encode(idStr.getBytes()), BlockSearchMode.UNCOMMITTED);
				if (blocksBeforeUpload.contains(entry)) {
					blocksBeforeUpload.remove(entry);
				}
			}
			blockBlob.commitBlockList(blocksBeforeUpload);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
Add Redis class to support reading/writing from Azure Redis Cache:
•	Create a new file Redis.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
public class Redis {
	static final String host = "hanzredis1.redis.cache.windows.net";
	static final String password = "eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=";

	static public void flushDB() {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.flushDB();
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static public String get(String key) {
		String value = null;
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			value = jedis.get(key);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		return value;
	}

	static public void set(String key, String value) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.set(key, value);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static public List<String> getList(String key, int maxLength) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		List<String> stringList = null;
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			stringList = jedis.lrange(key, 0, maxLength - 1);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		return stringList;
	}

	static public void setList(String key, List<String> stringList) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.del(key);
			for (String str : stringList) {
				jedis.lpush(key, str);
			}
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static void putMap(String key, Map<String, String> map) {
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.hmset(key, map);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static Map<String, String> getMap(String key) {
		Map<String, String> map = null;
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth(password);
		jedis.connect();
		if (jedis.isConnected()) {
			map = jedis.hgetAll(key);
		} else {
			System.out.println("connection error");
		}
		jedis.close();
		return map;
	}
}
Add ByteAggregator class to perform partitionAggregate operation:
•	Create a new file ByteAggregator.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlobState> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlobState.class);
	private int maxBlockBytes = 1024; // 1024 * 1024 * 4 / 16;

	private long txid;
	private int partitionIndex;
	private Properties properties;

	public ByteAggregator(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		this.partitionIndex = context.getPartitionIndex();
		super.prepare(conf, context);
	}

	public BlobState init(Object batchId, TridentCollector collector) {
		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		}
		this.maxBlockBytes = getMaxBlockBytes();
		int maxNumberOfBlocks = getMaxNumberOfblocks();
		BlobState state = new BlobState(this.partitionIndex, this.txid, maxNumberOfBlocks);
		//BlobWriter.remove(this.properties, state.blockIdStrFormat, state.block.blobname, state.block.blockidStr);
		return state;
	}

	public void aggregate(BlobState state, TridentTuple tuple, TridentCollector collector) {

		String tupleStr = tuple.getString(0);
		if (tupleStr != null && tupleStr.length() > 0) {
			String msg = tupleStr + "\r\n";
			if (msg.getBytes().length > this.maxBlockBytes) {
				throw new FailedException();
			}

			if ((state.block.blockdata + msg).getBytes().length <= this.maxBlockBytes) {//if fits within a block
				state.block.addData(msg);
			} else { // upload data, and then go to next block
				logger.info("Upload Block");
				state.block.upload(this.properties);
				state.needPersist = true;
				state.buildNextblock();
				state.block.addData(msg);
			}
		}
	}

	public void complete(BlobState state, TridentCollector collector) {
		if (state.block.blockdata.length() > 0) {
			state.block.upload(this.properties);  //upload the last block
			state.needPersist = true;
		}
		
		if (state.needPersist) {
			state.persist();
		}
		collector.emit(new Values(1));  //just emit a value
	}

	private int getMaxNumberOfblocks() {
		int maxNumberOfBlocks = 10;
		String maxNumberOfBlocksStr = properties.getProperty("storage.blob.block.number.max");
		if (maxNumberOfBlocksStr != null) {
			maxNumberOfBlocks = Integer.parseInt(maxNumberOfBlocksStr);
		}
		return maxNumberOfBlocks;
	}

	private int getMaxBlockBytes() {
		int maxBlockBytes = 1024;
		String maxBlockBytesStr = properties.getProperty("storage.blob.block.kb.max");
		if (maxBlockBytesStr != null) {
			maxBlockBytes = Integer.parseInt(maxBlockBytesStr) * 1024;
		}
		return maxBlockBytes;
	}
}
Add BlobState class:
•	Create a new file BlobState.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobState {
	public Block block;
	public boolean needPersist = false;

	private int maxNumberOfBlocks = 6;
	private static final String blobidBlockidStrFormat = "%05d_%05d";
	private static final String blobNameFormat = "aaa/blobwriter/%05d/%05d";
	public String blockIdStrFormat = "%05d";
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlobState.class);

	private String key_txid;
	private String key_blocklist;

	private int partitionIndex;
	private long txid;
	private List<String> blocklist;

	public BlobState(int partitionIndex, long txid, int maxNumberOfBlocks) {
		this.maxNumberOfBlocks = maxNumberOfBlocks;
		this.partitionIndex = partitionIndex;
		this.txid = txid;
		this.key_txid = "txid_" + String.valueOf(partitionIndex);
		this.key_blocklist = "blocklist_" + String.valueOf(partitionIndex);;
		this.blocklist = new ArrayList<String>();
		
		String lastTxidStr = Redis.get(this.key_txid);
		if (lastTxidStr == null) { // the very first time the topology is running
			this.block = new Block();
		} else {
			long lastTxid = Long.parseLong(lastTxidStr);
			if (txid != lastTxid) { // a new batch, not a replay
				this.block = getNextBlock(getLastBlock());
			} else {// if(txid == lastTxid) a replay, overwrite old block
				this.block = getFirstBlock();
			}
		}
		buildBlock();
	}

	public void buildNextblock() {
		this.block = getNextBlock(this.block);
		buildBlock();
	}

	public void persist() {
		Redis.set(this.key_txid, String.valueOf(this.txid));
		Redis.setList(this.key_blocklist, this.blocklist);

		logger.info(this.key_txid + " = " + this.txid);
		for (String s : this.blocklist) {
			logger.info(this.key_blocklist + " = " + s);
		}
	}

	private void buildBlock() {
		this.block.blockdata = new String("");
		this.block.blobname = String.format(blobNameFormat, this.partitionIndex, this.block.blobid);
		this.block.blockidStr = String.format(blockIdStrFormat, this.block.blockid);
		String blobidBlockidStr = String.format(blobidBlockidStrFormat, this.block.blobid, this.block.blockid);
		this.blocklist.add(blobidBlockidStr);
	}

	private Block getLastBlock() {
		Block block = new Block();
		List<String> lastBlobidBlockidList = Redis.getList(this.key_blocklist, 50000);
		if (lastBlobidBlockidList != null && lastBlobidBlockidList.size() > 0) {
			String blobidBlockidStr = lastBlobidBlockidList.get(0);
			for (String s : lastBlobidBlockidList) {
				if (s.compareTo(blobidBlockidStr) > 0) {// find the last block written in the last batch
					blobidBlockidStr = s;
				}
			}
			String[] strArray = blobidBlockidStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
		}
		return block;
	}

	private Block getFirstBlock() {
		Block block = new Block();
		List<String> lastblocks = Redis.getList(this.key_blocklist, 50000);
		if (lastblocks != null && lastblocks.size() > 0) {
			String blockStr = lastblocks.get(0);
			for (String s : lastblocks) {
				if (s.compareTo(blockStr) < 0) {// find the first block written in the last batch
					blockStr = s;
				}
			}
			String[] strArray = blockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
		}
		return block;
	}

	private Block getNextBlock(Block current) {
		Block next = new Block();
		if (current.blockid < maxNumberOfBlocks) {
			next.blobid = current.blobid;
			next.blockid = current.blockid + 1;
		} else {
			next.blobid = current.blobid + 1;
			next.blockid = 1;
		}
		return next;
	}

	public class Block {
		int blobid = 1;
		int blockid = 1;
		public String blobname;
		public String blockidStr;
		public String blockdata;
		
		public Block(){
			blobid = 1;
			blockid = 1;
			blockdata = "";
		}
		public void addData(String data) {
			blockdata = blockdata + data;
		}
		
		public String getData() {
			return blockdata;
		}

		public void upload(Properties properties) {
			BlobWriter.upload(properties, this.blobname, this.blockidStr, this.blockdata);
		}
	}
}
Add TestSpout class:
•	Create a new file TestSpout.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class TestSpout implements ITridentSpout<Long> {
    private static final long serialVersionUID = 1L;
    BatchCoordinator<Long> coordinator = new TestCoordinator();
    Emitter<Long> emitter = new TestEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("message");
    }
}

Add TestCoordinator class:
•	Create a new file TestSpout.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
package com.contoso.app.trident;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout.BatchCoordinator;
import java.io.Serializable;
public class TestCoordinator implements BatchCoordinator<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class);

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
        LOG.info("Initializing Transaction [" + txid + "]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful Transaction [" + txid + "]");
    }
}
 Add TestEmitter class:
•	Create a new file EventHubTestEmitter.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
import com.google.gson.Gson;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
public class TestEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    AtomicInteger successfulTransactions = new AtomicInteger(0);

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        for (int i = 0; i < 10000; i++) {
            List<Object> eventJsons = new ArrayList<Object>();
            double lat = new Double(-30 + (int) (Math.random() * 75));
            double lng = new Double(-120 + (int) (Math.random() * 70));
            long time = System.currentTimeMillis();

            String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
            EventHubMessage event = new EventHubMessage(lat, lng, time, diag);
            String eventJson = new Gson().toJson(event);

            eventJsons.add(eventJson);
            collector.emit(eventJsons);
        }
    }

    @Override
    public void success(TransactionAttempt tx) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {
    }
}
Add BlobWriterTopology class:
•	Create a new file BlobWriterTopology.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\
package com.contoso.app.trident;
package com.contoso.app.trident;
import java.io.FileReader;
import java.util.Properties;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import com.microsoft.eventhubs.trident.OpaqueTridentEventHubSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class BlobWriterTopology{
	public static void main(String[] args) throws Exception {
		Redis.flushDB();
		StormTopology stormTopology = buildStormTopology(args);

		if ((args != null) && (args.length > 0)) {
			// if running in storm cluster
			Config config = new Config();
			int numWorkers = getNumWorkers(args);
			config.setNumWorkers(numWorkers);
			System.out.println("Number of workers = " + numWorkers);
			// config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1L);
			StormSubmitter.submitTopology(args[0], config, stormTopology);
		} else {
			// if running in local development environment
			Config config = new Config();
			config.setMaxTaskParallelism(10);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("localTopology", config, stormTopology);
			Thread.sleep(5000000L);
			localCluster.shutdown();
		}
	}

	static StormTopology buildStormTopology(String[] args) throws Exception {
		Properties properties = new Properties();
		if ((args != null) && args.length > 1 && !args[1].toLowerCase().equals("test")) {
			System.out.println("Loding config file from file " + args[1]);
			properties.load(new FileReader(args[1]));
		} else {
			System.out.println("Loding config file from Config.properties");
			properties.load(BlobWriterTopology.class.getClassLoader().getResourceAsStream("Config.properties"));
		}

		TridentTopology tridentTopology = new TridentTopology();
		Stream inputStream = null;

		boolean useEventHubSpout = true;
		if ((args != null) && args.length > 1 && args[1].toLowerCase().equals("test")) {
			useEventHubSpout = false;
		}

		// useEventHubSpout = false;

		int numWorkers = getNumWorkers(args);

		if (useEventHubSpout) {
			System.out.println("useEventHubSpout = " + useEventHubSpout);
			OpaqueTridentEventHubSpout spout = createOpaqueTridentEventHubSpout(args);
			inputStream = tridentTopology.newStream("message", spout);
		} else {
			System.out.println("useEventHubSpout = " + useEventHubSpout);
			TestSpout spout = new TestSpout();
			inputStream = tridentTopology.newStream("message", spout);
		}

		inputStream.parallelismHint(numWorkers)
		.partitionAggregate(new Fields("message"),new ByteAggregator(properties), new Fields("blobname"));

		return tridentTopology.build();
	}

	static OpaqueTridentEventHubSpout createOpaqueTridentEventHubSpout(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig = readEHConfig(args);
		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
		return spout;
	}

	static EventHubSpoutConfig readEHConfig(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig;
		Properties properties = new Properties();
		if ((args != null) && args.length > 1 && !args[1].toLowerCase().equals("test")) {
			System.out.println("Loding config file from file " + args[1]);
			properties.load(new FileReader(args[1]));
		} else {
			System.out.println("Loding config file from Config.properties");
			properties.load(BlobWriterTopology.class.getClassLoader().getResourceAsStream("Config.properties"));
		}
		String username = properties.getProperty("eventhubspout.username");
		String password = properties.getProperty("eventhubspout.password");
		String namespaceName = properties.getProperty("eventhubspout.namespace");
		String entityPath = properties.getProperty("eventhubspout.entitypath");
		String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
		int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
		int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
		int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
		System.out.println("Eventhub spout config: ");
		System.out.println("  username: " + username);
		System.out.println("  password: " + password);
		System.out.println("  namespaceName: " + namespaceName);
		System.out.println("  entityPath: " + entityPath);
		System.out.println("  zkEndpointAddress: " + zkEndpointAddress);
		System.out.println("  partition count: " + partitionCount);
		System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
		System.out.println("  receiver credits: " + receiverCredits);
		spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress, checkpointIntervalInSeconds, receiverCredits);
		if ((args != null) && args.length > 0) {
			spoutConfig.setTopologyName(args[0]);
		}
		return spoutConfig;
	}

	static int getNumWorkers(String[] args) throws Exception {
		Properties properties = new Properties();
		if ((args != null) && args.length > 1 && !args[1].toLowerCase().equals("test")) {
			// read properties from the file specified by storm command line
			properties.load(new FileReader(args[1]));
		} else {
			// read properties from the Config.properties file
			properties.load(BlobWriterTopology.class.getClassLoader().getResourceAsStream("Config.properties"));
		}
		return Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
	}
}

Test the topology locally
To compile and test the file on your development machine, use the following steps.
•	Start the SendEvent .NET application to begin sending events, so that we have something to read from Event Hub.
•	Start the topology locally using the following command
•	mvn compile exec:java -Dstorm.topology=com.contoso.app.trident.BlobWriterTopology
•	This will start the topology, read messages from Event Hub, and upload them to azure blob storage
•	After verifying that this works, stop the topology by entering Ctrl-C. To stop the SendEvent app, select the window and press any key.
Package and deploy the topology to HDInsight
On your development environment, use the following steps to run the Temperature topology on your HDInsight Storm Cluster.
1.	Use the following command to create a JAR package from your project.
mvn package
This will create a file named eventhub-blobwriter-1.0-SNAPSHOT.jar in the target directory of your project.
2.	On your local development machine, start the SendEvents .NET application, so that we have some events to read.
3.	Connect to your HDInsight Storm cluster using Remote Desktop, and copy the jar file to the c:\apps\dist\storm<version number> directory.
4.	Use the HDInsight Command Line icon on the cluster desktop to open a new command prompt, and use the following commands to run the topology.
cd %storm_home%
bin\storm jar eventhub-blobwriter-1.0-SNAPSHOT.jar com.contoso.app.trident.BlobWriterTopology  BlobWriterTopology  
5.	To stop the topology, go to the Remote Desktop session with the Storm cluster and enter the following in the HDInsight Command Line.
6.	bin\storm kill BlobWriterTopology  
