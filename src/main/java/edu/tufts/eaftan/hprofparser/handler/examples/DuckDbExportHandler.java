/*
 * Handler to export parsed data to a DuckDB database.
 * Requires DuckDB JDBC dependency in your build system.
 */
package edu.tufts.eaftan.hprofparser.handler.examples;

import edu.tufts.eaftan.hprofparser.handler.NullRecordHandler;
import edu.tufts.eaftan.hprofparser.parser.datastructures.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DuckDbExportHandler extends NullRecordHandler {

	private Connection conn;

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
				"CREATE TABLE IF NOT EXISTS instances (objId BIGINT, stackTraceSerialNum INT, classObjId BIGINT, instanceFieldValues VARCHAR)");
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
	}

	@Override
	public void header(String format, int idSize, long time) {
		// TODO: Export header info to DuckDB
	}

	@Override
	public void stringInUTF8(long id, String data) {
		// Export string data to DuckDB
		stringCount++;
		if (stringCount % 1000 == 0) {
			System.out.println("[DuckDbExport] Exported " + stringCount + " strings");
		}
		try (PreparedStatement ps = conn
				.prepareStatement("INSERT INTO strings (id, data) VALUES (?, ?) ON CONFLICT (id) DO NOTHING")) {
			ps.setLong(1, id);
			ps.setString(2, data);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void loadClass(int classSerialNum, long classObjId, int stackTraceSerialNum, long classNameStringId) {
		// Export class info to DuckDB
		classCount++;
		if (classCount % 1000 == 0) {
			System.out.println("[DuckDbExport] Exported " + classCount + " classes");
		}
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO classes (classSerialNum, classObjId, stackTraceSerialNum, classNameStringId) VALUES (?, ?, ?, ?)")) {
			ps.setInt(1, classSerialNum);
			ps.setLong(2, classObjId);
			ps.setInt(3, stackTraceSerialNum);
			ps.setLong(4, classNameStringId);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startThread(int threadSerialNum, long threadObjectId, int stackTraceSerialNum, long threadNameStringId,
			long threadGroupNameId, long threadParentGroupNameId) {
		// Export thread info to DuckDB
		try {
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO threads (threadSerialNum, threadObjectId, stackTraceSerialNum, threadNameStringId, threadGroupNameId, threadParentGroupNameId) VALUES (?, ?, ?, ?, ?, ?)")) {
				ps.setInt(1, threadSerialNum);
				ps.setLong(2, threadObjectId);
				ps.setInt(3, stackTraceSerialNum);
				ps.setLong(4, threadNameStringId);
				ps.setLong(5, threadGroupNameId);
				ps.setLong(6, threadParentGroupNameId);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void heapSummary(int totalLiveBytes, int totalLiveInstances, long totalBytesAllocated,
			long totalInstancesAllocated) {
		// Export heap summary to DuckDB
		try {
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO heap_summary (totalLiveBytes, totalLiveInstances, totalBytesAllocated, totalInstancesAllocated) VALUES (?, ?, ?, ?)")) {
				ps.setInt(1, totalLiveBytes);
				ps.setInt(2, totalLiveInstances);
				ps.setLong(3, totalBytesAllocated);
				ps.setLong(4, totalInstancesAllocated);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void instanceDump(long objId, int stackTraceSerialNum, long classObjId, Value<?>[] instanceFieldValues) {
		// Export instance dump to DuckDB
		instanceCount++;
		if (instanceCount % 1000 == 0) {
			System.out.println("[DuckDbExport] Exported " + instanceCount + " instances");
		}
		try {
			// Serialize instanceFieldValues as a comma-separated string
			StringBuilder sb = new StringBuilder();
			if (instanceFieldValues != null) {
				for (int i = 0; i < instanceFieldValues.length; i++) {
					sb.append(instanceFieldValues[i]);
					if (i < instanceFieldValues.length - 1)
						sb.append(",");
				}
			}
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO instances (objId, stackTraceSerialNum, classObjId, instanceFieldValues) VALUES (?, ?, ?, ?)")) {
				ps.setLong(1, objId);
				ps.setInt(2, stackTraceSerialNum);
				ps.setLong(3, classObjId);
				ps.setString(4, sb.toString());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void classDump(long classObjId, int stackTraceSerialNum, long superClassObjId, long classLoaderObjId,
			long signersObjId, long protectionDomainObjId, long reserved1, long reserved2, int instanceSize,
			Constant[] constants, Static[] statics, InstanceField[] instanceFields) {
		// Export class dump to DuckDB
		classDumpCount++;
		if (classDumpCount % 1000 == 0) {
			System.out.println("[DuckDbExport] Exported " + classDumpCount + " class dumps");
		}
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
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO class_dumps (classObjId, stackTraceSerialNum, superClassObjId, classLoaderObjId, signersObjId, protectionDomainObjId, reserved1, reserved2, instanceSize, constants, statics, instanceFields) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
				ps.setLong(1, classObjId);
				ps.setInt(2, stackTraceSerialNum);
				ps.setLong(3, superClassObjId);
				ps.setLong(4, classLoaderObjId);
				ps.setLong(5, signersObjId);
				ps.setLong(6, protectionDomainObjId);
				ps.setLong(7, reserved1);
				ps.setLong(8, reserved2);
				ps.setInt(9, instanceSize);
				ps.setString(10, constantsSb.toString());
				ps.setString(11, staticsSb.toString());
				ps.setString(12, fieldsSb.toString());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void objArrayDump(long objId, int stackTraceSerialNum, long elemClassObjId, long[] elems) {
		// Export object array dump to DuckDB
		objArrayCount++;
		if (objArrayCount % 1000 == 0) {
			System.out.println("[DuckDbExport] Exported " + objArrayCount + " object arrays");
		}
		try {
			StringBuilder sb = new StringBuilder();
			if (elems != null) {
				for (int i = 0; i < elems.length; i++) {
					sb.append(elems[i]);
					if (i < elems.length - 1)
						sb.append(",");
				}
			}
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO obj_arrays (objId, stackTraceSerialNum, elemClassObjId, elems) VALUES (?, ?, ?, ?)")) {
				ps.setLong(1, objId);
				ps.setInt(2, stackTraceSerialNum);
				ps.setLong(3, elemClassObjId);
				ps.setString(4, sb.toString());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void primArrayDump(long objId, int stackTraceSerialNum, byte elemType, Value<?>[] elems) {
		// Export primitive array dump to DuckDB
		primArrayCount++;
		if (primArrayCount % 1000 == 0) {
			System.out.println("[DuckDbExport] Exported " + primArrayCount + " primitive arrays");
		}
		try {
			StringBuilder sb = new StringBuilder();
			if (elems != null) {
				for (int i = 0; i < elems.length; i++) {
					sb.append(elems[i]);
					if (i < elems.length - 1)
						sb.append(",");
				}
			}
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO prim_arrays (objId, stackTraceSerialNum, elemType, elems) VALUES (?, ?, ?, ?)")) {
				ps.setLong(1, objId);
				ps.setInt(2, stackTraceSerialNum);
				ps.setShort(3, elemType);
				ps.setString(4, sb.toString());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
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

	// Helper for root* records
	private void exportRoot(String table, Object... values) {
		try {
			// Build table and insert statement dynamically
			StringBuilder create = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
			StringBuilder insert = new StringBuilder("INSERT INTO ").append(table).append(" VALUES (");
			for (int i = 0; i < values.length; i++) {
				create.append("col").append(i).append(" BIGINT");
				insert.append("?");
				if (i < values.length - 1) {
					create.append(", ");
					insert.append(", ");
				}
			}
			create.append(")");
			insert.append(")");
			conn.createStatement().executeUpdate(create.toString());
			try (PreparedStatement ps = conn.prepareStatement(insert.toString())) {
				for (int i = 0; i < values.length; i++) {
					ps.setObject(i + 1, values[i]);
				}
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Example for closing connection
	public void close() throws SQLException {
		if (conn != null && !conn.isClosed()) {
			conn.close();
		}
	}
}
