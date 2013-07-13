package fengfei.forest.database.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface KeyValueTransducer<E, T> {

	KeyValue<E, T> transform(ResultSet rs) throws SQLException;

	public static class KeyValue<E, T> {
		public E key;
		public T value;

		public KeyValue(E key, T value) {
			super();
			this.key = key;
			this.value = value;
		}

	}
}
