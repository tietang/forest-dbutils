package fengfei.forest.database.pool;

import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp.AbandonedConfig;
import org.apache.commons.dbcp.AbandonedObjectPool;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.pool.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fengfei.forest.database.utils.ParamsUtils;

/**
 * 
 * <pre>
 * http://commons.apache.org/proper/commons-dbcp/configuration.html
 * <bean id="dataSource" class="org.apache.commons.dbcp2.BasicDataSource" init-method="init" destroy-method="close">
 *     <!-- 基本属性 url、user、password -->
 *     <property name="url" value="${jdbc_url}" />
 *     <property name="username" value="${jdbc_user}" />
 *     <property name="password" value="${jdbc_password}" />
 *       
 *     <!-- 配置初始化大小、最小、最大 -->
 *     <property name="initialSize" value="1" />
 *     <property name="minIdle" value="1" />
 *     <property name="maxActive" value="20" />
 *  
 *     <!-- 配置获取连接等待超时的时间 -->
 *     <property name="maxWait" value="60000" />
 *  
 *     <!-- 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒 -->
 *     <property name="timeBetweenEvictionRunsMillis" value="60000" />
 *  
 *     <!-- 配置一个连接在池中最小生存的时间，单位是毫秒 -->
 *     <property name="minEvictableIdleTimeMillis" value="300000" />
 *   
 *     <property name="validationQuery" value="SELECT 'x'" />
 *     <property name="testWhileIdle" value="true" />
 *     <property name="testOnBorrow" value="false" />
 *     <property name="testOnReturn" value="false" />
 *  
 *     <!-- 打开PSCache，并且指定每个连接上PSCache的大小 -->
 *     <property name="poolPreparedStatements" value="true" />
 *     <property name="maxPoolPreparedStatementPerConnectionSize" value="20" />
 *  
 *     如何配置关闭长时间不使用的连接
 *     <property name="removeAbandoned" value="true" /> <!-- 打开removeAbandoned功能 -->
 *     <property name="removeAbandonedTimeout" value="1800" /> <!-- 1800秒，也就是30分钟 -->
 *     <property name="logAbandoned" value="true" /> <!-- 关闭abanded连接时输出错误日志 -->
 *     <!-- 配置监控统计拦截的filters -->
 *     <property name="filters" value="stat" />
 * </bean>
 * </pre>
 * 
 * @author tietang
 * 
 */
public class DbcpPoolableDataSourceFactory implements PoolableDataSourceFactory {

	private static final Logger logger = LoggerFactory
			.getLogger(DbcpPoolableDataSourceFactory.class);

	@Override
	public DataSource createDataSource(String driverClass, String url,
			String user, String password, Map<String, String> params)
			throws PoolableException {

		DataSource ds = null;
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
				url, user, password);
		try {
			AbandonedConfig conf = new AbandonedConfig();
			conf.setLogAbandoned(ParamsUtils.getDefaultBoolean(params,
					"logAbandoned", false));
			conf.setRemoveAbandoned(ParamsUtils.getDefaultBoolean(params,
					"removeAbandoned", false));
			conf.setRemoveAbandonedTimeout(ParamsUtils.getDefaultInt(params,
					"removeAbandonedTimeout", 60));
			ObjectPool<?> connectionPool = null;
			// if (conf.getLogAbandoned() && conf.getRemoveAbandoned()) {
			// for watch connections
			connectionPool = new AbandonedObjectPool(null, conf);
			BeanUtils.copyProperties(connectionPool, params);

			// }
			new PoolableConnectionFactory(connectionFactory, connectionPool,
					null, ParamsUtils.getValidationQuery(params),
					ParamsUtils.getDefaultBoolean(params, "defaultReadOnly"),
					ParamsUtils.getDefaultBoolean(params, "defaultAutoCommit"),
					conf);
			ds = new ClosablePoolingDataSource(connectionPool);

		} catch (Exception e) {
			logger.error("create DBCP ClosablePoolingDataSource error", e);
			throw new PoolableException(
					"create DBCP ClosablePoolingDataSource error", e);

		}

		return ds;

	}

	@Override
	public void destory(DataSource dataSource) throws PoolableException {
		if (dataSource == null) {
			return;
		}
		try {
			ClosablePoolingDataSource ds = (ClosablePoolingDataSource) dataSource;
			ds.close();
		} catch (Throwable e) {
			throw new PoolableException(
					"destory DBCP ClosablePoolingDataSource error", e);
		}

	}

}
