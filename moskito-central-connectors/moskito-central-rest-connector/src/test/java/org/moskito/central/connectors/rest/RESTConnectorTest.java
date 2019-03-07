package org.moskito.central.connectors.rest;

import java.util.HashMap;

import org.junit.Test;
import org.moskito.central.Snapshot;
import org.moskito.central.SnapshotMetaData;

import com.sun.jersey.test.framework.JerseyTest;

/**
 * Test class for MoSKito Central REST-connector.
 *
 * @author Vladyslav Bezuhlyi
 */
public class RESTConnectorTest extends JerseyTest {

	/**
	 * Exposing sending method to test the connector.
	 */
	public static class ExposedRESTConnector extends RESTConnector {

		public ExposedRESTConnector() {
			super.setConfigurationName("rest-connector");
		}


		@Override
		public void sendData(Snapshot snapshot) {
			super.sendData(snapshot);
		}

	}


	public RESTConnectorTest() {
		super("org.moskito.central.connectors.rest", "org.codehaus.jackson.jaxrs");
	}


	@Override
	protected int getPort(int defaultPort) {
		return super.getPort(9988);
	}


	@Test
	public void testAddSnapshot() throws InterruptedException {
		Snapshot snapshot = new Snapshot();

		SnapshotMetaData metaData = new SnapshotMetaData();
		metaData.setProducerId("prodId");
		metaData.setCategory("catId");
		metaData.setSubsystem("subSId");

		snapshot.setMetaData(metaData);

		HashMap<String, String> data = new HashMap<String, String>();
		data.put("firstname", "moskito");
		data.put("lastname", "central");
		snapshot.addSnapshotData("test", data);
		snapshot.addSnapshotData("test2", data);
		snapshot.addSnapshotData("test3", data);

		ExposedRESTConnector connector = new ExposedRESTConnector();
		connector.sendData(snapshot);
		Thread.sleep(1000);
	}

}
