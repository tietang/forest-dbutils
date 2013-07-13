package fengfei.forest.database.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbutils.ResultSetHandler;

import fengfei.forest.database.dbutils.KeyValueTransducer.KeyValue;

public class KeyValueHandler<E, T> implements ResultSetHandler<Map<E, T>> {

	private KeyValueTransducer<E, T> keyValueTransducer;

	public KeyValueHandler(KeyValueTransducer<E, T> transducer) {
		this.keyValueTransducer = transducer;
	}

	protected KeyValue<E, T> handleRow(ResultSet rs) throws SQLException {
		KeyValue<E, T> kv = keyValueTransducer.transform(rs);
		return kv;
	}

	@Override
	public Map<E, T> handle(ResultSet rs) throws SQLException {
		Map<E, T> map = new HashMap<>();
		while (rs.next()) {
			KeyValue<E, T> kv = handleRow(rs);
			map.put(kv.key, kv.value);
		}
		return map;
	}
}