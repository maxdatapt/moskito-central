package org.moskito.central.connectors.rest;

import org.configureme.annotations.ConfigureMe;
import org.moskito.central.connectors.AbstractCentralConnectorConfig;

/**
 * REST config bean.
 * 
 * @author dagafonov
 * 
 */
@ConfigureMe(allfields = true)
public class RESTConnectorConfig extends AbstractCentralConnectorConfig {

    /**
     * HTTP server host.
     */
    private String host;

    /**
     * HTTP server port.
     */
    private int port;

    /**
     * HTTP server path.
     */
    private String resourcePath;

    /**
     * Is HTTP basic auth enabled.
     */
    private boolean basicAuthEnabled;

    /**
     * Basic auth login.
     */
    private String login;

    /**
     * Basic auth password.
     */
    private String password;

    /**
     * If true: self-signed certificates are trusted even if they are not in the truststore.
     */
    private boolean trustSelfSigned;

    /**
     * Is host verification (URL & certificate's value) enabled.
     */
    private boolean hostVerificationEnabled;

    /**
     * Path to TrustStore file.
     */
    private String trustStoreFilePath;

    /**
     * TrustStore password.
     */
    private String trustStorePassword;
    
    /** The application name. */
	private String applicationName;

	private String proxyURI;
	private Integer proxyPort;
	private String proxyUsername;
	private String proxyPassword;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public boolean isBasicAuthEnabled() {
        return basicAuthEnabled;
    }

    public void setBasicAuthEnabled(boolean basicAuthEnabled) {
        this.basicAuthEnabled = basicAuthEnabled;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isTrustSelfSigned() {
        return trustSelfSigned;
    }

    public void setTrustSelfSigned(boolean trustSelfSigned) {
        this.trustSelfSigned = trustSelfSigned;
    }

    public boolean isHostVerificationEnabled() {
        return hostVerificationEnabled;
    }

    public void setHostVerificationEnabled(boolean hostVerificationEnabled) {
        this.hostVerificationEnabled = hostVerificationEnabled;
    }

    public String getTrustStoreFilePath() {
        return trustStoreFilePath;
    }

    public void setTrustStoreFilePath(String trustStoreFilePath) {
        this.trustStoreFilePath = trustStoreFilePath;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }
    
    
    public String getApplicationName() {
		return applicationName;
	}


	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}


	public String getProxyURI() {
		return proxyURI;
	}


	public void setProxyURI(String proxyURI) {
		this.proxyURI = proxyURI;
	}


	public String getProxyUsername() {
		return proxyUsername;
	}


	public void setProxyUsername(String proxyUsername) {
		this.proxyUsername = proxyUsername;
	}


	public String getProxyPassword() {
		return proxyPassword;
	}


	public void setProxyPassword(String proxyPassword) {
		this.proxyPassword = proxyPassword;
	}


	public Integer getProxyPort() {
		return proxyPort;
	}


	public void setProxyPort(Integer proxyPort) {
		this.proxyPort = proxyPort;
	}

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RESTConnectorConfig{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append(", resourcePath='").append(resourcePath).append('\'');
        sb.append(", basicAuthEnabled=").append(basicAuthEnabled);
        sb.append(", login='").append(login).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", trustSelfSigned=").append(trustSelfSigned);
        sb.append(", hostVerificationEnabled=").append(hostVerificationEnabled);
        sb.append(", trustStoreFilePath='").append(trustStoreFilePath).append('\'');
        sb.append(", trustStorePassword='").append(trustStorePassword).append('\'');
        sb.append(", supportedIntervals='").append(super.getSupportedIntervals()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
