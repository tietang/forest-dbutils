package fengfei.forest.database.pool;

import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fengfei.forest.database.utils.ParamsUtils;

/**
 * <pre>
 * config key/value:
 * 				 testWhileIdle="true"
 *               testOnBorrow="true"
 *               testOnReturn="false"
 *               validationQuery="SELECT 1"
 *               validationInterval="30000"
 *               timeBetweenEvictionRunsMillis="30000"
 *               maxActive="100" 
 *               minIdle="10" 
 *               maxWait="10000" 
 *               initialSize="10"
 *               removeAbandonedTimeout="60"
 *               removeAbandoned="true"
 *               logAbandoned="true"
 *               minEvictableIdleTimeMillis="30000" 
 *               jmxEnabled="true"
 *               jdbcInterceptors="org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer"
 *               username="root" 
 *               password="password" 
 *               driverClassName="com.mysql.jdbc.Driver"
 *               url="jdbc:mysql://localhost:3306/mysql"
 * 
 * </per>
 * @author tietang
 * 
 */
public class TomcatPoolableDataSourceFactory implements
		PoolableDataSourceFactory {

	private static final Logger logger = LoggerFactory
			.getLogger(TomcatPoolableDataSourceFactory.class);

	@Override
	public DataSource createDataSource(String driverClass, String url,
			String user, String password, Map<String, String> params)
			throws PoolableException {

		org.apache.tomcat.jdbc.pool.DataSource ds = null;
		try {
			PoolProperties poolProperties = new PoolProperties();
			poolProperties.setLogAbandoned(ParamsUtils.getDefaultBoolean(
					params, "logAbandoned", false));
			poolProperties.setRemoveAbandoned(ParamsUtils.getDefaultBoolean(
					params, "removeAbandoned", false));
			poolProperties.setRemoveAbandonedTimeout(ParamsUtils.getDefaultInt(
					params, "removeAbandonedTimeout", 60));
			BeanUtils.copyProperties(poolProperties, params);
			poolProperties
					.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
							+ "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
			poolProperties.setDriverClassName(driverClass);
			poolProperties.setUrl(url);
			poolProperties.setUsername(user);
			poolProperties.setPassword(password);
			ds = new org.apache.tomcat.jdbc.pool.DataSource();
			ds.setPoolProperties(poolProperties);
		} catch (Exception e) {
			logger.error("create tomcat.jdbc.pool.DataSource  error", e);
			throw new PoolableException(
					"create tomcat.jdbc.pool.DataSource  error", e);
		}

		return ds;

	}

	@Override
	public void destory(DataSource dataSource) throws PoolableException {
		if (dataSource == null) {
			return;
		}
		try {
			org.apache.tomcat.jdbc.pool.DataSource ds = (org.apache.tomcat.jdbc.pool.DataSource) dataSource;
			ds.close();
		} catch (Throwable e) {
			throw new PoolableException(
					"destory tomcat.jdbc.pool.DataSource error", e);
		}

	}

}
