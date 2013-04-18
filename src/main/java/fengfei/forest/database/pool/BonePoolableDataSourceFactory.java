package fengfei.forest.database.pool;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

/**
 * <pre>
 * 
 * <!-- BoneCP configuration -->
 * <bean id="mainDataSource" class="com.jolbox.bonecp.BoneCPDataSource" destroy-method="close">
 *    <property name="driverClass" value="com.mysql.jdbc.Driver" />
 *    <property name="jdbcUrl" value="jdbc:mysql://127.0.0.1/yourdb" />
 *    <property name="username" value="root"/>
 *    <property name="password" value="abcdefgh"/>
 *    <property name="idleConnectionTestPeriod" value="60"/>
 *    <property name="idleMaxAge" value="240"/>
 *    <property name="maxConnectionsPerPartition" value="30"/>
 *    <property name="minConnectionsPerPartition" value="10"/>
 *    <property name="partitionCount" value="3"/>
 *    <property name="acquireIncrement" value="5"/>
 *    <property name="statementsCacheSize" value="100"/>
 *    <property name="releaseHelperThreads" value="3"/>
 * </bean>
 * 
 * 
 * </pre>
 * 
 * @author tietang
 * 
 */
public class BonePoolableDataSourceFactory implements PoolableDataSourceFactory {

	@Override
	public DataSource createDataSource(String driverClass, String url,
			String user, String password, Map<String, String> params)
			throws PoolableException {
		try {
			Class.forName(driverClass); // load the DB driver
			BoneCPConfig config = new BoneCPConfig();
			BeanUtils.copyProperties(config, params);
			BoneCPDataSource ds = new BoneCPDataSource(config);
			ds.setJdbcUrl(url); // set the JDBC url
			ds.setUsername(user); // set the username
			ds.setPassword(password); // set the password
			
			return ds;
		} catch (ClassNotFoundException | IllegalAccessException
				| InvocationTargetException e) {
			throw new PoolableException("create BoneCPDataSource error", e);
		}
	}

	@Override
	public void destory(DataSource dataSource) throws PoolableException {
		if (dataSource == null) {
			return;
		}
		try {
			BoneCPDataSource ds = (BoneCPDataSource) dataSource;
			ds.close();
		} catch (Throwable e) {
			throw new PoolableException("destory BoneCPDataSource error", e);
		}

	}

}
