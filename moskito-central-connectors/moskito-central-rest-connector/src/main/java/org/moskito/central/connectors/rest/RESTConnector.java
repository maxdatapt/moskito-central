package org.moskito.central.connectors.rest;

import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import org.configureme.ConfigurationManager;
import org.moskito.central.Snapshot;
import org.moskito.central.connectors.AbstractCentralConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

/**
 * REST connector implementation to the Central.
 *
 * @author dagafonov
 */
public class RESTConnector extends AbstractCentralConnector {

	/**
	 * Logger instance.
	 */
	private final static Logger log = LoggerFactory.getLogger(RESTConnector.class);

	/**
	 * Connector config instance.
	 */
	private RESTConnectorConfig connectorConfig;

    /**
     * Cached client instance.
     */
    private volatile Client client;

	/**
	 * Default constructor.
	 */
	public RESTConnector() {
		super();
	}


    @Override
    public void setConfigurationName(String configurationName) {
        connectorConfig = new RESTConnectorConfig();
        ConfigurationManager.INSTANCE.configureAs(connectorConfig, configurationName);
        super.configure(connectorConfig);

        log.debug("Config: " + connectorConfig);
        client = getClient();
    }

    @Override
    protected void sendData(Snapshot snapshot) {
         // hammer down app name
		snapshot.getMetaData().setApplicationName(connectorConfig.getApplicationName());

		WebResource resource = client.resource(getBaseURI());
		resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).post(snapshot);
    }

    private Client getClient() {
		Client client = null;

		if (connectorConfig.getProxyURI() != null) {

			// set proxy authentication if we are using it
			if (connectorConfig.getProxyUsername() != null) {
				Authenticator.setDefault(
						new Authenticator() {
							@Override
							public PasswordAuthentication getPasswordAuthentication() {
								return new PasswordAuthentication(
										connectorConfig.getProxyUsername(), connectorConfig.getProxyPassword().toCharArray());
							}
						});

				System.setProperty("http.proxyHost", connectorConfig.getProxyURI());
				System.setProperty("http.proxyPort", connectorConfig.getProxyPort() + "");
				System.setProperty("http.proxyUser", connectorConfig.getProxyUsername());
				System.setProperty("http.proxyPassword", connectorConfig.getProxyPassword());

				System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
			}


			URLConnectionClientHandler ch = new URLConnectionClientHandler(new ConnectionFactory());
			client = new Client(ch, getClientConfig());
		} else {
			client = Client.create(getClientConfig());
		}

		if (connectorConfig.isBasicAuthEnabled()) {
			/* adding HTTP basic auth header to request */
			client.addFilter(new HTTPBasicAuthFilter(connectorConfig.getLogin(), connectorConfig.getPassword()));
		}

		return client;
	}

    protected ClientConfig getClientConfig() {
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getClasses().add(JacksonJaxbJsonProvider.class);
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        return clientConfig;
    }

    protected URI getBaseURI() {
        return UriBuilder.fromUri("http://" + connectorConfig.getHost() + connectorConfig.getResourcePath()).port(connectorConfig.getPort()).build();
    }

    protected RESTConnectorConfig getConnectorConfig(){
        return connectorConfig;
    }
    
    public class ConnectionFactory implements HttpURLConnectionFactory {

		Proxy proxy;


		private void initializeProxy() {
			proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(connectorConfig.getProxyURI(), connectorConfig.getProxyPort()));
		}


		@Override
		public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
			initializeProxy();
			return (HttpURLConnection) url.openConnection(proxy);
		}
	}

}
