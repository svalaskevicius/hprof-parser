/*
 * Handler to export parsed data to a DuckDB database with batching for performance.
 * Requires DuckDB JDBC dependency in your build system.
 */
package edu.tufts.eaftan.hprofparser.handler.examples;

import edu.tufts.eaftan.hprofparser.handler.NullRecordHandler;
import edu.tufts.eaftan.hprofparser.parser.datastructures.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class DuckDbExportHandler extends NullRecordHandler {

	private Connection conn;
	private static final int BATCH_SIZE = 1000;

	// Map from classObjId to its instance fields
	private Map<Long, InstanceField[]> classInstanceFields = new HashMap<>();
	// Map from stringId to actual string value
	private Map<Long, String> stringIdToValue = new HashMap<>();

	// Batched prepared statements
	private PreparedStatement stringBatch;
	private PreparedStatement classBatch;
	private PreparedStatement instanceBatch;
	private PreparedStatement instanceFieldBatch;
	private PreparedStatement classDumpBatch;
	private PreparedStatement objArrayBatch;
	private PreparedStatement primArrayBatch;

	// Batch counters
	private int stringBatchCount = 0;
	private int classBatchCount = 0;
	private int instanceBatchCount = 0;
	private int instanceFieldBatchCount = 0;
	private int classDumpBatchCount = 0;
	private int objArrayBatchCount = 0;
	private int primArrayBatchCount = 0;

	// Progress counters
	private long stringCount = 0;
	private long classCount = 0;
	private long instanceCount = 0;
	private long classDumpCount = 0;
	private long objArrayCount = 0;
	private long primArrayCount = 0;

	public DuckDbExportHandler(String dbPath) throws SQLException {
		// Connect to DuckDB database file
		conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
		conn.setAutoCommit(false); // Enable manual transaction control for batching

		// Precreate all tables needed for export
		conn.createStatement()
				.executeUpdate("CREATE TABLE IF NOT EXISTS strings (id BIGINT PRIMARY KEY, data VARCHAR)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS classes (classSerialNum INT, classObjId BIGINT, stackTraceSerialNum INT, classNameStringId BIGINT)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS threads (threadSerialNum INT, threadObjectId BIGINT, stackTraceSerialNum INT, threadNameStringId BIGINT, threadGroupNameId BIGINT, threadParentGroupNameId BIGINT)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS heap_summary (totalLiveBytes INT, totalLiveInstances INT, totalBytesAllocated BIGINT, totalInstancesAllocated BIGINT)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS instances (objId BIGINT, stackTraceSerialNum INT, classObjId BIGINT)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS instance_fields (instanceObjId BIGINT, fieldName VARCHAR, fieldType VARCHAR, fieldValue VARCHAR)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS class_dumps (classObjId BIGINT, stackTraceSerialNum INT, superClassObjId BIGINT, classLoaderObjId BIGINT, signersObjId BIGINT, protectionDomainObjId BIGINT, reserved1 BIGINT, reserved2 BIGINT, instanceSize INT, constants VARCHAR, statics VARCHAR, instanceFields VARCHAR)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS obj_arrays (objId BIGINT, stackTraceSerialNum INT, elemClassObjId BIGINT, elems VARCHAR)");
		conn.createStatement().executeUpdate(
				"CREATE TABLE IF NOT EXISTS prim_arrays (objId BIGINT, stackTraceSerialNum INT, elemType SMALLINT, elems VARCHAR)");

		// Precreate all root tables
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS root_unknown (col0 BIGINT)");
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS root_jni_global (col0 BIGINT, col1 BIGINT)");
		conn.createStatement()
				.executeUpdate("CREATE TABLE IF NOT EXISTS root_jni_local (col0 BIGINT, col1 BIGINT, col2 BIGINT)");
		conn.createStatement()
				.executeUpdate("CREATE TABLE IF NOT EXISTS root_java_frame (col0 BIGINT, col1 BIGINT, col2 BIGINT)");
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS root_native_stack (col0 BIGINT, col1 BIGINT)");
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS root_sticky_class (col0 BIGINT)");
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS root_thread_block (col0 BIGINT, col1 BIGINT)");
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS root_monitor_used (col0 BIGINT)");
		conn.createStatement()
				.executeUpdate("CREATE TABLE IF NOT EXISTS root_thread_obj (col0 BIGINT, col1 BIGINT, col2 BIGINT)");

		conn.commit();

		// Initialize batched prepared statements
		stringBatch = conn.prepareStatement("INSERT INTO strings (id, data) VALUES (?, ?) ON CONFLICT (id) DO NOTHING");
		classBatch = conn.prepareStatement(
				"INSERT INTO classes (classSerialNum, classObjId, stackTraceSerialNum, classNameStringId) VALUES (?, ?, ?, ?)");
		instanceBatch = conn
				.prepareStatement("INSERT INTO instances (objId, stackTraceSerialNum, classObjId) VALUES (?, ?, ?)");
		instanceFieldBatch = conn.prepareStatement(
				"INSERT INTO instance_fields (instanceObjId, fieldName, fieldType, fieldValue) VALUES (?, ?, ?, ?)");
		classDumpBatch = conn.prepareStatement(
				"INSERT INTO class_dumps (classObjId, stackTraceSerialNum, superClassObjId, classLoaderObjId, signersObjId, protectionDomainObjId, reserved1, reserved2, instanceSize, constants, statics, instanceFields) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		objArrayBatch = conn.prepareStatement(
				"INSERT INTO obj_arrays (objId, stackTraceSerialNum, elemClassObjId, elems) VALUES (?, ?, ?, ?)");
		primArrayBatch = conn.prepareStatement(
				"INSERT INTO prim_arrays (objId, stackTraceSerialNum, elemType, elems) VALUES (?, ?, ?, ?)");
	}

	@Override
	public void header(String format, int idSize, long time) {
		// TODO: Export header info to DuckDB
	}

	@Override
	public void stringInUTF8(long id, String data) {
		stringCount++;
		stringIdToValue.put(id, data);

		try {
			stringBatch.setLong(1, id);
			stringBatch.setString(2, data);
			stringBatch.addBatch();
			stringBatchCount++;

			if (stringBatchCount >= BATCH_SIZE) {
				flushStringBatch();
			}

			if (stringCount % 10000 == 0) {
				System.out.println("[DuckDbExport] Exported " + stringCount + " strings");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void loadClass(int classSerialNum, long classObjId, int stackTraceSerialNum, long classNameStringId) {
		classCount++;

		try {
			classBatch.setInt(1, classSerialNum);
			classBatch.setLong(2, classObjId);
			classBatch.setInt(3, stackTraceSerialNum);
			classBatch.setLong(4, classNameStringId);
			classBatch.addBatch();
			classBatchCount++;

			if (classBatchCount >= BATCH_SIZE) {
				flushClassBatch();
			}

			if (classCount % 10000 == 0) {
				System.out.println("[DuckDbExport] Exported " + classCount + " classes");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startThread(int threadSerialNum, long threadObjectId, int stackTraceSerialNum, long threadNameStringId,
			long threadGroupNameId, long threadParentGroupNameId) {
		// Threads are typically not numerous, so we can insert directly
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO threads (threadSerialNum, threadObjectId, stackTraceSerialNum, threadNameStringId, threadGroupNameId, threadParentGroupNameId) VALUES (?, ?, ?, ?, ?, ?)")) {
			ps.setInt(1, threadSerialNum);
			ps.setLong(2, threadObjectId);
			ps.setInt(3, stackTraceSerialNum);
			ps.setLong(4, threadNameStringId);
			ps.setLong(5, threadGroupNameId);
			ps.setLong(6, threadParentGroupNameId);
			ps.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void heapSummary(int totalLiveBytes, int totalLiveInstances, long totalBytesAllocated,
			long totalInstancesAllocated) {
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO heap_summary (totalLiveBytes, totalLiveInstances, totalBytesAllocated, totalInstancesAllocated) VALUES (?, ?, ?, ?)")) {
			ps.setInt(1, totalLiveBytes);
			ps.setInt(2, totalLiveInstances);
			ps.setLong(3, totalBytesAllocated);
			ps.setLong(4, totalInstancesAllocated);
			ps.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void instanceDump(long objId, int stackTraceSerialNum, long classObjId, Value<?>[] instanceFieldValues) {
		instanceCount++;

		try {
			instanceBatch.setLong(1, objId);
			instanceBatch.setInt(2, stackTraceSerialNum);
			instanceBatch.setLong(3, classObjId);
			instanceBatch.addBatch();
			instanceBatchCount++;

			if (instanceBatchCount >= BATCH_SIZE) {
				flushInstanceBatch();
			}

			// Insert normalized instance fields
			InstanceField[] fields = classInstanceFields.get(classObjId);
			if (fields != null && instanceFieldValues != null && fields.length == instanceFieldValues.length) {
				for (int i = 0; i < fields.length; i++) {
					instanceFieldBatch.setLong(1, objId);
					instanceFieldBatch.setString(2, stringIdToValue.getOrDefault(fields[i].fieldNameStringId,
							String.valueOf(fields[i].fieldNameStringId)));
					instanceFieldBatch.setString(3, fields[i].type.toString());
					instanceFieldBatch.setString(4, String.valueOf(instanceFieldValues[i]));
					instanceFieldBatch.addBatch();
					instanceFieldBatchCount++;

					if (instanceFieldBatchCount >= BATCH_SIZE) {
						flushInstanceFieldBatch();
					}
				}
			}

			if (instanceCount % 10000 == 0) {
				System.out.println("[DuckDbExport] Exported " + instanceCount + " instances");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void classDump(long classObjId, int stackTraceSerialNum, long superClassObjId, long classLoaderObjId,
			long signersObjId, long protectionDomainObjId, long reserved1, long reserved2, int instanceSize,
			Constant[] constants, Static[] statics, InstanceField[] instanceFields) {
		classInstanceFields.put(classObjId, instanceFields);
		classDumpCount++;

		try {
			// Serialize arrays
			StringBuilder constantsSb = new StringBuilder();
			if (constants != null) {
				for (int i = 0; i < constants.length; i++) {
					constantsSb.append(constants[i].constantPoolIndex).append(":").append(constants[i].value);
					if (i < constants.length - 1)
						constantsSb.append(",");
				}
			}
			StringBuilder staticsSb = new StringBuilder();
			if (statics != null) {
				for (int i = 0; i < statics.length; i++) {
					staticsSb.append(statics[i].staticFieldNameStringId).append(":").append(statics[i].value);
					if (i < statics.length - 1)
						staticsSb.append(",");
				}
			}
			StringBuilder fieldsSb = new StringBuilder();
			if (instanceFields != null) {
				for (int i = 0; i < instanceFields.length; i++) {
					fieldsSb.append(instanceFields[i].fieldNameStringId).append(":").append(instanceFields[i].type);
					if (i < instanceFields.length - 1)
						fieldsSb.append(",");
				}
			}

			classDumpBatch.setLong(1, classObjId);
			classDumpBatch.setInt(2, stackTraceSerialNum);
			classDumpBatch.setLong(3, superClassObjId);
			classDumpBatch.setLong(4, classLoaderObjId);
			classDumpBatch.setLong(5, signersObjId);
			classDumpBatch.setLong(6, protectionDomainObjId);
			classDumpBatch.setLong(7, reserved1);
			classDumpBatch.setLong(8, reserved2);
			classDumpBatch.setInt(9, instanceSize);
			classDumpBatch.setString(10, constantsSb.toString());
			classDumpBatch.setString(11, staticsSb.toString());
			classDumpBatch.setString(12, fieldsSb.toString());
			classDumpBatch.addBatch();
			classDumpBatchCount++;

			if (classDumpBatchCount >= BATCH_SIZE) {
				flushClassDumpBatch();
			}

			if (classDumpCount % 10000 == 0) {
				System.out.println("[DuckDbExport] Exported " + classDumpCount + " class dumps");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void objArrayDump(long objId, int stackTraceSerialNum, long elemClassObjId, long[] elems) {
		objArrayCount++;

		try {
			StringBuilder sb = new StringBuilder();
			if (elems != null) {
				for (int i = 0; i < elems.length; i++) {
					sb.append(elems[i]);
					if (i < elems.length - 1)
						sb.append(",");
				}
			}

			objArrayBatch.setLong(1, objId);
			objArrayBatch.setInt(2, stackTraceSerialNum);
			objArrayBatch.setLong(3, elemClassObjId);
			objArrayBatch.setString(4, sb.toString());
			objArrayBatch.addBatch();
			objArrayBatchCount++;

			if (objArrayBatchCount >= BATCH_SIZE) {
				flushObjArrayBatch();
			}

			if (objArrayCount % 10000 == 0) {
				System.out.println("[DuckDbExport] Exported " + objArrayCount + " object arrays");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void primArrayDump(long objId, int stackTraceSerialNum, byte elemType, Value<?>[] elems) {
		primArrayCount++;

		try {
			StringBuilder sb = new StringBuilder();
			if (elems != null) {
				for (int i = 0; i < elems.length; i++) {
					sb.append(elems[i]);
					if (i < elems.length - 1)
						sb.append(",");
				}
			}

			primArrayBatch.setLong(1, objId);
			primArrayBatch.setInt(2, stackTraceSerialNum);
			primArrayBatch.setShort(3, elemType);
			primArrayBatch.setString(4, sb.toString());
			primArrayBatch.addBatch();
			primArrayBatchCount++;

			if (primArrayBatchCount >= BATCH_SIZE) {
				flushPrimArrayBatch();
			}

			if (primArrayCount % 10000 == 0) {
				System.out.println("[DuckDbExport] Exported " + primArrayCount + " primitive arrays");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Batch flush methods
	private void flushStringBatch() throws SQLException {
		if (stringBatchCount > 0) {
			stringBatch.executeBatch();
			conn.commit();
			stringBatchCount = 0;
		}
	}

	private void flushClassBatch() throws SQLException {
		if (classBatchCount > 0) {
			classBatch.executeBatch();
			conn.commit();
			classBatchCount = 0;
		}
	}

	private void flushInstanceBatch() throws SQLException {
		if (instanceBatchCount > 0) {
			instanceBatch.executeBatch();
			conn.commit();
			instanceBatchCount = 0;
		}
	}

	private void flushInstanceFieldBatch() throws SQLException {
		if (instanceFieldBatchCount > 0) {
			instanceFieldBatch.executeBatch();
			conn.commit();
			instanceFieldBatchCount = 0;
		}
	}

	private void flushClassDumpBatch() throws SQLException {
		if (classDumpBatchCount > 0) {
			classDumpBatch.executeBatch();
			conn.commit();
			classDumpBatchCount = 0;
		}
	}

	private void flushObjArrayBatch() throws SQLException {
		if (objArrayBatchCount > 0) {
			objArrayBatch.executeBatch();
			conn.commit();
			objArrayBatchCount = 0;
		}
	}

	private void flushPrimArrayBatch() throws SQLException {
		if (primArrayBatchCount > 0) {
			primArrayBatch.executeBatch();
			conn.commit();
			primArrayBatchCount = 0;
		}
	}

	// Export all root* records
	@Override
	public void rootUnknown(long objId) {
		exportRoot("root_unknown", objId);
	}

	@Override
	public void rootJNIGlobal(long objId, long JNIGlobalRefId) {
		exportRoot("root_jni_global", objId, JNIGlobalRefId);
	}

	@Override
	public void rootJNILocal(long objId, int threadSerialNum, int frameNum) {
		exportRoot("root_jni_local", objId, threadSerialNum, frameNum);
	}

	@Override
	public void rootJavaFrame(long objId, int threadSerialNum, int frameNum) {
		exportRoot("root_java_frame", objId, threadSerialNum, frameNum);
	}

	@Override
	public void rootNativeStack(long objId, int threadSerialNum) {
		exportRoot("root_native_stack", objId, threadSerialNum);
	}

	@Override
	public void rootStickyClass(long objId) {
		exportRoot("root_sticky_class", objId);
	}

	@Override
	public void rootThreadBlock(long objId, int threadSerialNum) {
		exportRoot("root_thread_block", objId, threadSerialNum);
	}

	@Override
	public void rootMonitorUsed(long objId) {
		exportRoot("root_monitor_used", objId);
	}

	@Override
	public void rootThreadObj(long objId, int threadSerialNum, int stackTraceSerialNum) {
		exportRoot("root_thread_obj", objId, threadSerialNum, stackTraceSerialNum);
	}

	// Helper for root* records (kept as-is since roots are typically not numerous)
	private void exportRoot(String table, Object... values) {
		try {
			StringBuilder insert = new StringBuilder("INSERT INTO ").append(table).append(" VALUES (");
			for (int i = 0; i < values.length; i++) {
				insert.append("?");
				if (i < values.length - 1) {
					insert.append(", ");
				}
			}
			insert.append(")");

			try (PreparedStatement ps = conn.prepareStatement(insert.toString())) {
				for (int i = 0; i < values.length; i++) {
					ps.setObject(i + 1, values[i]);
				}
				ps.executeUpdate();
			}
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Close connection and flush remaining batches
	public void close() throws SQLException {
		try {
			// Flush all remaining batches
			flushStringBatch();
			flushClassBatch();
			flushInstanceBatch();
			flushInstanceFieldBatch();
			flushClassDumpBatch();
			flushObjArrayBatch();
			flushPrimArrayBatch();

			// Close prepared statements
			if (stringBatch != null)
				stringBatch.close();
			if (classBatch != null)
				classBatch.close();
			if (instanceBatch != null)
				instanceBatch.close();
			if (instanceFieldBatch != null)
				instanceFieldBatch.close();
			if (classDumpBatch != null)
				classDumpBatch.close();
			if (objArrayBatch != null)
				objArrayBatch.close();
			if (primArrayBatch != null)
				primArrayBatch.close();

			// Close connection
			if (conn != null && !conn.isClosed()) {
				conn.commit();
				conn.close();
			}

			System.out.println("[DuckDbExport] Export complete. Final counts:");
			System.out.println("  Strings: " + stringCount);
			System.out.println("  Classes: " + classCount);
			System.out.println("  Instances: " + instanceCount);
			System.out.println("  Class Dumps: " + classDumpCount);
			System.out.println("  Object Arrays: " + objArrayCount);
			System.out.println("  Primitive Arrays: " + primArrayCount);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}
}
