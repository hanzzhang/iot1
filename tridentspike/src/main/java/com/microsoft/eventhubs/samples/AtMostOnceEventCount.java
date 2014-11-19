package com.microsoft.eventhubs.samples;

import com.microsoft.eventhubs.spout.EventHubSpout;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import com.microsoft.eventhubs.spout.IEventHubReceiver;
import com.microsoft.eventhubs.spout.IPartitionManager;
import com.microsoft.eventhubs.spout.IPartitionManagerFactory;
import com.microsoft.eventhubs.spout.IStateStore;
import com.microsoft.eventhubs.spout.SimplePartitionManager;
import java.io.Serializable;

public class AtMostOnceEventCount extends EventCount implements Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	protected EventHubSpout createEventHubSpout() {
		IPartitionManagerFactory pmFactory = new IPartitionManagerFactory() {
			private static final long serialVersionUID = 1L;

			@Override
			public IPartitionManager create(EventHubSpoutConfig spoutConfig, String partitionId, String startingOffset, IStateStore stateStore,
					IEventHubReceiver receiver) {
				return new SimplePartitionManager(spoutConfig, partitionId, startingOffset, stateStore, receiver);
			}
		};
		EventHubSpout eventHubSpout = new EventHubSpout(this.spoutConfig, null, pmFactory, null);

		return eventHubSpout;
	}

	public static void main(String[] args) throws Exception {
		AtMostOnceEventCount scenario = new AtMostOnceEventCount();

		scenario.runScenario(args);
	}
}
