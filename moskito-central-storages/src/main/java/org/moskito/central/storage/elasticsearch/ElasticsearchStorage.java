package org.moskito.central.storage.elasticsearch;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.configureme.ConfigurationManager;
import org.elasticsearch.client.Client;
import org.moskito.central.Snapshot;
import org.moskito.central.SnapshotTypeConverted;
import org.moskito.central.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;


/**
 * @author andriiskrypnyk
 */
public class ElasticsearchStorage implements Storage {

	private static Logger log = LoggerFactory.getLogger(ElasticsearchStorage.class);
	/**
	 * Storage config.
	 */
	private ElasticsearchStorageConfig config;
	/**
	 * Transport client if java api is used
	 */
	private Client transportClient;

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();


	private HashSet<String> preparedIndexes = new HashSet<>();


	@Override
	public void configure(String configurationName) {

		config = new ElasticsearchStorageConfig();

		if (configurationName == null)
			return;
		try {
			ConfigurationManager.INSTANCE.configureAs(config, configurationName);
		} catch (IllegalArgumentException e) {
			log.warn("Couldn't configure ElasticsearchStorage with " + configurationName + " , working with default values");
		}

		if (config.getProxy().equals("true") && config.getApi().equals("http")) {
			config.setPort("");
		} else if (!config.getApi().equals("java")) {
			config.setHost(config.getHost() + ":");
		}

	}


	@Override
	public void processSnapshot(Snapshot target) {

		String producerId = target.getMetaData().getProducerId();
		String interval = target.getMetaData().getIntervalName();

		if (!config.include(producerId, interval)) {
			return;
		}

		httpProcessSnapshot(target);

	}


	public static boolean isBigDecimal(String b) {
		if (b == null) {
			return false;
		}

		try {
			b = b.replaceAll(",", ".");
			new BigDecimal(b);
			return true;
		} catch (NumberFormatException nfe) {
		}
		return false;
	}


	/**
	 * To snake case.
	 *
	 * @param string the string
	 * @return the string
	 */
	private String toSnakeCase(String string) {
		String regex = "([a-z])([A-Z]+)";
		String replacement = "$1_$2";
		return string.replaceAll(" ", "").replaceAll(regex, replacement).toLowerCase();
	}


	private void httpProcessSnapshot(Snapshot target) {
		String indexName = toSnakeCase(target.getMetaData().getApplicationName()) + "_" + toSnakeCase(target.getMetaData().getProducerId());

		if (!preparedIndexes.contains(indexName)) {
			httpPrepareIndex(indexName);
			preparedIndexes.add(indexName);
		}


		CloseableHttpClient httpClientb = null;
		HttpPost post = new HttpPost(config.getHost() + config.getPort() + "/" + indexName + "/" + indexName);
		try {


			SnapshotTypeConverted aaa = new SnapshotTypeConverted();
			aaa.setMetaData(target.getMetaData());

			aaa.getStats();

			for (Map.Entry<String, Map<String, String>> entry : target.getStats().entrySet()) {
				String key = entry.getKey();
				Map<String, String> val = entry.getValue();
				Map<String, Object> newVal = new HashMap<>();

				for (Map.Entry<String, String> v : val.entrySet()) {
					String k = v.getKey();

					Object value = isBigDecimal(v.getValue()) ? new BigDecimal(v.getValue()) : v.getValue();

					if (v.getValue() == null) {
						continue;
					} else if (v.getValue().equals("NaN")) {
						value = new BigDecimal(0);
					} else if (isBigDecimal(v.getValue())) {
						if (((BigDecimal) value).compareTo(new BigDecimal(9000000000000000000L)) == 1 || ((BigDecimal) value).compareTo(new BigDecimal(-9000000000000000000L)) == -1) {
							value = new BigDecimal(0);
						}
					}

					newVal.put(k, value);
					log.debug(key + "-" + k + ":converted numeric:" + value + ", it was: " + v.getValue());
				}

				aaa.getStats().put(key, newVal);
			}


			StringEntity entity = new StringEntity(gson.toJson(aaa));
			entity.setContentType("application/json");
			post.setEntity(entity);

			httpClientb = HttpClients.createDefault();
			CloseableHttpResponse response = httpClientb.execute(post);
			log.debug(response.toString());
		} catch (

		Exception e) {
			log.error("Couldn't process Snapshot: ", e);
		} finally {
			try {
				httpClientb.close();
			} catch (Exception e) {
			}
		}


	}


	private void httpPrepareIndex(String indexName) {

		CloseableHttpClient httpClienta = HttpClients.createDefault();
		//		HttpConnectionParams.setConnectionTimeout(httpClient.getParams(), 30000);

		JsonObject json = new JsonObject();
		JsonObject mappings = new JsonObject();
		JsonObject application = new JsonObject();
		JsonObject parentProperties = new JsonObject();
		JsonObject metaData = new JsonObject();
		JsonObject properties = new JsonObject();
		JsonObject arrivalTimestamp = new JsonObject();
		JsonObject creationTimestamp = new JsonObject();

		// date fields
		creationTimestamp.addProperty("type", "date");
		arrivalTimestamp.addProperty("type", "date");

		properties.add("creationTimestamp", creationTimestamp);
		properties.add("arrivalTimestamp", arrivalTimestamp);
		metaData.add("properties", properties);
		parentProperties.add("metaData", metaData);
		application.add("properties", parentProperties);
		mappings.add(indexName, application);
		json.add("mappings", mappings);

		try {
			HttpPut put = new HttpPut(config.getHost() + config.getPort() + "/" + indexName);
			StringEntity entity = new StringEntity(json.toString());
			entity.setContentType("application/json");
			put.setEntity(entity);

			httpClienta.execute(put);
		} catch (IOException e) {
			log.error("Can't prepare index", e);
		}

		try {
			httpClienta.close();
		} catch (Exception e) {
		}
	}
}
