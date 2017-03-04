package org.apache.karaf.main.lock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.felix.utils.properties.Properties;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.util.StringUtil;

/**
 * The Class HazelcastLock.
 * 
 * @author Rupesh Raut
 * @version 1.0
 */
public class HazelcastLock implements Lock {

	/** The logger. */
	private final Logger logger = Logger.getLogger(HazelcastLock.class.getName());

	/** The Constant PROP_KARAF_LOCK_NAME. */
	private static final String PROP_KARAF_LOCK_NAME = "karaf.lock.name";

	/** The Constant PROP_KARAF_LOCK_TIMEOUT. */
	private static final String PROP_KARAF_LOCK_TIMEOUT = "karaf.lock.timeout";

	/** The Constant PROP_KARAF_HAZELCAST_SERVER_CONFIG_PATH. */
	private static final String PROP_KARAF_HAZELCAST_SERVER_CONFIG_PATH = "karaf.lock.hazelcast.server.config.path";

	/** The Constant PROP_KARAF_HAZELCAST_CLIENT_CONFIG_PATH. */
	private static final String PROP_KARAF_HAZELCAST_CLIENT_CONFIG_PATH = "karaf.lock.hazelcast.client.config.path";

	/** The properties. */
	private final Properties properties;

	/** The server instance. */
	private HazelcastInstance serverInstance;

	/** The client instance. */
	private HazelcastInstance clientInstance;

	/** The lock. */
	private ILock lock;

	/** The lock name. */
	private String lockName;

	/** The timeout. */
	private int timeout;

	/** The server config path. */
	private String serverConfigPath;

	/** The client config path. */
	private String clientConfigPath;

	/**
	 * Instantiates a new hazelcast lock.
	 *
	 * @param properties
	 *            the properties
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public HazelcastLock(Properties properties) throws IOException {
		super();
		this.properties = properties;
		init();
		server();
		client();
		this.lock = getServerInstance().getLock(getLockName());
	}

	/**
	 * Gets the properties.
	 *
	 * @return the properties
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Gets the server instance.
	 *
	 * @return the server instance
	 */
	public HazelcastInstance getServerInstance() {
		return serverInstance;
	}

	/**
	 * Sets the server instance.
	 *
	 * @param serverInstance
	 *            the new server instance
	 */
	public void setServerInstance(HazelcastInstance serverInstance) {
		this.serverInstance = serverInstance;
	}

	/**
	 * Gets the client instance.
	 *
	 * @return the client instance
	 */
	public HazelcastInstance getClientInstance() {
		return clientInstance;
	}

	/**
	 * Sets the client instance.
	 *
	 * @param clientInstance
	 *            the new client instance
	 */
	public void setClientInstance(HazelcastInstance clientInstance) {
		this.clientInstance = clientInstance;
	}

	/**
	 * Gets the lock.
	 *
	 * @return the lock
	 */
	public ILock getLock() {
		return lock;
	}

	/**
	 * Sets the lock.
	 *
	 * @param lock
	 *            the new lock
	 */
	public void setLock(ILock lock) {
		this.lock = lock;
	}

	/**
	 * Gets the lock name.
	 *
	 * @return the lock name
	 */
	public String getLockName() {
		return lockName;
	}

	/**
	 * Sets the lock name.
	 *
	 * @param lockName
	 *            the new lock name
	 */
	public void setLockName(String lockName) {
		this.lockName = lockName;
	}

	/**
	 * Gets the timeout.
	 *
	 * @return the timeout
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Sets the timeout.
	 *
	 * @param timeout
	 *            the new timeout
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Gets the server config path.
	 *
	 * @return the server config path
	 */
	public String getServerConfigPath() {
		return serverConfigPath;
	}

	/**
	 * Sets the server config path.
	 *
	 * @param serverConfigPath
	 *            the new server config path
	 */
	public void setServerConfigPath(String serverConfigPath) {
		this.serverConfigPath = serverConfigPath;
	}

	/**
	 * Gets the client config path.
	 *
	 * @return the client config path
	 */
	public String getClientConfigPath() {
		return clientConfigPath;
	}

	/**
	 * Sets the client config path.
	 *
	 * @param clientConfigPath
	 *            the new client config path
	 */
	public void setClientConfigPath(String clientConfigPath) {
		this.clientConfigPath = clientConfigPath;
	}

	/**
	 * Inits the.
	 */
	protected void init() {
		this.lockName = getProperties().getOrDefault(PROP_KARAF_LOCK_NAME, "DEFAULT_LOCK");
		this.timeout = Integer.valueOf(getProperties().getOrDefault(PROP_KARAF_LOCK_TIMEOUT, "10"));
		this.serverConfigPath = getProperties().getOrDefault(PROP_KARAF_HAZELCAST_SERVER_CONFIG_PATH, "");
		this.clientConfigPath = getProperties().getOrDefault(PROP_KARAF_HAZELCAST_CLIENT_CONFIG_PATH, "");
		logger.log(Level.INFO, "property setup complete ...");
	}

	/**
	 * Server.
	 *
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	protected void server() throws FileNotFoundException {
		if (StringUtil.isNullOrEmpty(getServerConfigPath())) {
			serverInstance = Hazelcast.newHazelcastInstance();
		} else {
			serverInstance = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(""));
		} // if-else

		logger.log(Level.INFO, "hazelcast server started ...");
	}

	/**
	 * Client.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	protected void client() throws IOException {
		if (StringUtil.isNullOrEmpty(getClientConfigPath())) {
			clientInstance = HazelcastClient.newHazelcastClient();
		} else {
			clientInstance = HazelcastClient.newHazelcastClient(new XmlClientConfigBuilder("").build());
		}
		logger.log(Level.INFO, "hazelcast client started ...");
	}

	/**
	 * Shutdown server.
	 */
	protected void shutdownServer() {
		getServerInstance().shutdown();
		logger.log(Level.INFO, "hazelcast server shutdown complete");
	}

	/**
	 * Shutdown client.
	 */
	protected void shutdownClient() {
		getClientInstance().shutdown();
		logger.log(Level.INFO, "hazelcast client shutdown complete");
	}

	/**
	 * Lock.
	 *
	 * @return true, if successful
	 * @throws Exception
	 *             the exception
	 * @see org.apache.karaf.main.lock.Lock#lock()
	 */
	@Override
	public boolean lock() throws Exception {
		if (getLock().tryLock()) {
			logger.log(Level.INFO, "lock accuired...");
		} // if
		return getLock().isLockedByCurrentThread();
	}// lock()

	/**
	 * Release.
	 *
	 * @throws Exception
	 *             the exception
	 * @see org.apache.karaf.main.lock.Lock#release()
	 */
	@Override
	public void release() throws Exception {
		if (getLock().isLockedByCurrentThread()) {
			getLock().unlock();
			logger.log(Level.INFO, "lock released....");
		} // if
	}// release()

	/**
	 * Checks if is alive.
	 *
	 * @return true, if is alive
	 * @throws Exception
	 *             the exception
	 * @see org.apache.karaf.main.lock.Lock#isAlive()
	 */
	@Override
	public boolean isAlive() throws Exception {
		return getLock().isLockedByCurrentThread();
	}

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		HazelcastLock hazelcastLock = new HazelcastLock(properties);
		hazelcastLock.lock();

		TimeUnit.SECONDS.sleep(10);

		if (hazelcastLock.isAlive()) {
			hazelcastLock.release();
		}

		hazelcastLock.shutdownClient();
		hazelcastLock.shutdownServer();

	}

}
