package edu.tufts.eaftan.hprofparser.handler.examples;

import edu.tufts.eaftan.hprofparser.handler.NullRecordHandler;
import edu.tufts.eaftan.hprofparser.parser.datastructures.*;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Rewritten to use DuckDB Appender API for high-throughput bulk inserts. - One
 * Appender per target table (created once) - append(beginRow/append/endRow)
 * used for each row - periodic commits to control transaction size - arrays are
 * passed as Java arrays (Long[], String[], etc.) which DuckDB maps to LIST
 */
public class DuckDbExportHandler extends NullRecordHandler {

	private Connection conn;
	private DuckDBConnection dconn;
	private String dbPath;

	// Performance settings
	private static final int COMMIT_INTERVAL = 100_000; // commit after this many rows overall

	// Map from classObjId to its instance fields
	private Map<Long, InstanceField[]> classInstanceFields = new ConcurrentHashMap<>();
	private Map<Long, String> stringIdToValue = new ConcurrentHashMap<>();

	// Appenders
	private DuckDBAppender stringsAppender;
	private DuckDBAppender classesAppender;
	private DuckDBAppender threadsAppender;
	private DuckDBAppender heapSummaryAppender;
	private DuckDBAppender instancesAppender;
	private DuckDBAppender classDumpsAppender;
	private DuckDBAppender objArraysAppender;

	// Type-specific primitive array tables
	private DuckDBAppender primArraysBooleanAppender;
	private DuckDBAppender primArraysCharAppender;
	private DuckDBAppender primArraysFloatAppender;
	private DuckDBAppender primArraysDoubleAppender;
	private DuckDBAppender primArraysByteAppender;
	private DuckDBAppender primArraysShortAppender;
	private DuckDBAppender primArraysIntAppender;
	private DuckDBAppender primArraysLongAppender;

	// Type-specific instance field tables
	private DuckDBAppender instanceFieldsObjectAppender;
	private DuckDBAppender instanceFieldsBooleanAppender;
	private DuckDBAppender instanceFieldsCharAppender;
	private DuckDBAppender instanceFieldsFloatAppender;
	private DuckDBAppender instanceFieldsDoubleAppender;
	private DuckDBAppender instanceFieldsByteAppender;
	private DuckDBAppender instanceFieldsShortAppender;
	private DuckDBAppender instanceFieldsIntAppender;
	private DuckDBAppender instanceFieldsLongAppender;

	// Root appenders
	private DuckDBAppender rootUnknownAppender;
	private DuckDBAppender rootJniGlobalAppender;
	private DuckDBAppender rootJniLocalAppender;
	private DuckDBAppender rootJavaFrameAppender;
	private DuckDBAppender rootNativeStackAppender;
	private DuckDBAppender rootStickyClassAppender;
	private DuckDBAppender rootThreadBlockAppender;
	private DuckDBAppender rootMonitorUsedAppender;
	private DuckDBAppender rootThreadObjAppender;

	// Progress counters
	private long stringCount = 0;
	private long classCount = 0;
	private long instanceCount = 0;
	private long classDumpCount = 0;
	private long objArrayCount = 0;
	private long primArrayCount = 0;
	private long totalRecordsProcessed = 0;

	// Reusable objects
	private ThreadLocal<StringBuilder> stringBuilderCache = ThreadLocal.withInitial(() -> new StringBuilder(8192));

	public DuckDbExportHandler(String dbPath) throws SQLException {
		this.dbPath = dbPath;
		try {
			conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
			conn.setAutoCommit(false);
			// DuckDB JDBC connection is also an org.duckdb.DuckDBConnection
			dconn = (DuckDBConnection) conn;

			// Performance tuning for DuckDB
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("PRAGMA threads=16");
				stmt.execute("PRAGMA memory_limit='64GB'");
				stmt.execute("SET preserve_insertion_order=false");
				stmt.execute("SET enable_object_cache=true");
				stmt.execute("PRAGMA enable_progress_bar=false");
				stmt.execute("PRAGMA force_compression='uncompressed'"); // Disable compression during import

				// Create tables WITHOUT indexes or constraints (add them later)
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS strings (id BIGINT, data VARCHAR)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS classes (classSerialNum INT, classObjId BIGINT, stackTraceSerialNum INT, classNameStringId BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS threads (threadSerialNum INT, threadObjectId BIGINT, stackTraceSerialNum INT, threadNameStringId BIGINT, threadGroupNameId BIGINT, threadParentGroupNameId BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS heap_summary (totalLiveBytes INT, totalLiveInstances INT, totalBytesAllocated BIGINT, totalInstancesAllocated BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instances (objId BIGINT, stackTraceSerialNum INT, classObjId BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS class_dumps (classObjId BIGINT, stackTraceSerialNum INT, superClassObjId BIGINT, classLoaderObjId BIGINT, signersObjId BIGINT, protectionDomainObjId BIGINT, reserved1 BIGINT, reserved2 BIGINT, instanceSize INT, constants VARCHAR, statics VARCHAR, instanceFields VARCHAR)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS obj_arrays (objId BIGINT, stackTraceSerialNum INT, elemClassObjId BIGINT, elems BIGINT[])");
				// Type-specific primitive array tables
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_boolean (objId BIGINT, stackTraceSerialNum INT, elems BOOLEAN[])");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_char (objId BIGINT, stackTraceSerialNum INT, elems VARCHAR)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_float (objId BIGINT, stackTraceSerialNum INT, elems FLOAT[])");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_double (objId BIGINT, stackTraceSerialNum INT, elems DOUBLE[])");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_byte (objId BIGINT, stackTraceSerialNum INT, elems TINYINT[])");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_short (objId BIGINT, stackTraceSerialNum INT, elems SMALLINT[])");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_int (objId BIGINT, stackTraceSerialNum INT, elems INTEGER[])");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS prim_arrays_long (objId BIGINT, stackTraceSerialNum INT, elems BIGINT[])");

				// Type-specific instance field tables
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_object (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_boolean (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue BOOLEAN)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_char (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue VARCHAR)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_float (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue FLOAT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_double (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue DOUBLE)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_byte (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue TINYINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_short (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue SMALLINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_int (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue INTEGER)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS instance_fields_long (instanceObjId BIGINT, fieldNameStringId BIGINT, fieldValue BIGINT)");

				// Root tables
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_unknown (objId BIGINT)");
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_jni_global (objId BIGINT, jniGlobalRefId BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS root_jni_local (objId BIGINT, threadSerialNum INT, frameNum INT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS root_java_frame (objId BIGINT, threadSerialNum INT, frameNum INT)");
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_native_stack (objId BIGINT, threadSerialNum INT)");
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_sticky_class (objId BIGINT)");
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_thread_block (objId BIGINT, threadSerialNum INT)");
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_monitor_used (objId BIGINT)");
				stmt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS root_thread_obj (objId BIGINT, threadSerialNum INT, stackTraceSerialNum INT)");
			}

			conn.commit();

			// Create one appender per table (namespace "main")
			stringsAppender = dconn.createAppender("main", "strings");
			classesAppender = dconn.createAppender("main", "classes");
			threadsAppender = dconn.createAppender("main", "threads");
			heapSummaryAppender = dconn.createAppender("main", "heap_summary");
			instancesAppender = dconn.createAppender("main", "instances");
			classDumpsAppender = dconn.createAppender("main", "class_dumps");
			objArraysAppender = dconn.createAppender("main", "obj_arrays");

			// Type-specific primitive array appenders
			primArraysBooleanAppender = dconn.createAppender("main", "prim_arrays_boolean");
			primArraysCharAppender = dconn.createAppender("main", "prim_arrays_char");
			primArraysFloatAppender = dconn.createAppender("main", "prim_arrays_float");
			primArraysDoubleAppender = dconn.createAppender("main", "prim_arrays_double");
			primArraysByteAppender = dconn.createAppender("main", "prim_arrays_byte");
			primArraysShortAppender = dconn.createAppender("main", "prim_arrays_short");
			primArraysIntAppender = dconn.createAppender("main", "prim_arrays_int");
			primArraysLongAppender = dconn.createAppender("main", "prim_arrays_long");

			// Type-specific appenders
			instanceFieldsObjectAppender = dconn.createAppender("main", "instance_fields_object");
			instanceFieldsBooleanAppender = dconn.createAppender("main", "instance_fields_boolean");
			instanceFieldsCharAppender = dconn.createAppender("main", "instance_fields_char");
			instanceFieldsFloatAppender = dconn.createAppender("main", "instance_fields_float");
			instanceFieldsDoubleAppender = dconn.createAppender("main", "instance_fields_double");
			instanceFieldsByteAppender = dconn.createAppender("main", "instance_fields_byte");
			instanceFieldsShortAppender = dconn.createAppender("main", "instance_fields_short");
			instanceFieldsIntAppender = dconn.createAppender("main", "instance_fields_int");
			instanceFieldsLongAppender = dconn.createAppender("main", "instance_fields_long");

			rootUnknownAppender = dconn.createAppender("main", "root_unknown");
			rootJniGlobalAppender = dconn.createAppender("main", "root_jni_global");
			rootJniLocalAppender = dconn.createAppender("main", "root_jni_local");
			rootJavaFrameAppender = dconn.createAppender("main", "root_java_frame");
			rootNativeStackAppender = dconn.createAppender("main", "root_native_stack");
			rootStickyClassAppender = dconn.createAppender("main", "root_sticky_class");
			rootThreadBlockAppender = dconn.createAppender("main", "root_thread_block");
			rootMonitorUsedAppender = dconn.createAppender("main", "root_monitor_used");
			rootThreadObjAppender = dconn.createAppender("main", "root_thread_obj");

			System.out.println("[DuckDB] Database initialized with Appender API and performance optimizations");
		} catch (SQLException e) {
			// ensure partial resources cleaned up
			closeQuiet();
			throw e;
		}
	}

	@Override
	public void header(String format, int idSize, long time) {
		// no-op
	}

	@Override
	public void stringInUTF8(long id, String data) {
		stringCount++;
		stringIdToValue.put(id, data);
		try {
			stringsAppender.beginRow();
			stringsAppender.append(id);
			stringsAppender.append(data);
			stringsAppender.endRow();

			totalRecordsProcessed++;
			if (stringCount % 50_000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", stringCount) + " strings");
			}

			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void loadClass(int classSerialNum, long classObjId, int stackTraceSerialNum, long classNameStringId) {
		classCount++;
		try {
			classesAppender.beginRow();
			classesAppender.append(classSerialNum);
			classesAppender.append(classObjId);
			classesAppender.append(stackTraceSerialNum);
			classesAppender.append(classNameStringId);
			classesAppender.endRow();

			totalRecordsProcessed++;
			if (classCount % 10_000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", classCount) + " classes");
			}
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startThread(int threadSerialNum, long threadObjectId, int stackTraceSerialNum, long threadNameStringId,
			long threadGroupNameId, long threadParentGroupNameId) {
		try {
			threadsAppender.beginRow();
			threadsAppender.append(threadSerialNum);
			threadsAppender.append(threadObjectId);
			threadsAppender.append(stackTraceSerialNum);
			threadsAppender.append(threadNameStringId);
			threadsAppender.append(threadGroupNameId);
			threadsAppender.append(threadParentGroupNameId);
			threadsAppender.endRow();
			totalRecordsProcessed++;
			conn.commit(); // threads are small and infrequent; commit immediately to avoid holding locks
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void heapSummary(int totalLiveBytes, int totalLiveInstances, long totalBytesAllocated,
			long totalInstancesAllocated) {
		try {
			heapSummaryAppender.beginRow();
			heapSummaryAppender.append(totalLiveBytes);
			heapSummaryAppender.append(totalLiveInstances);
			heapSummaryAppender.append(totalBytesAllocated);
			heapSummaryAppender.append(totalInstancesAllocated);
			heapSummaryAppender.endRow();
			totalRecordsProcessed++;
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void instanceDump(long objId, int stackTraceSerialNum, long classObjId,
			InstanceFieldWithValue<?>[] instanceFieldValues) {
		instanceCount++;
		try {
			instancesAppender.beginRow();
			instancesAppender.append(objId);
			instancesAppender.append(stackTraceSerialNum);
			instancesAppender.append(classObjId);
			instancesAppender.endRow();
			totalRecordsProcessed++;

			for (int i = 0; i < instanceFieldValues.length; i++) {
				insertTypedField(objId, instanceFieldValues[i]);
			}

			if (instanceCount % 50_000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", instanceCount) + " instances");
			}
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void insertTypedField(long instanceObjId, InstanceFieldWithValue<?> value) throws Exception {
		long fieldNameStringId = value.field.fieldNameStringId;

		switch (value.field.type) {
		case Type.OBJ:
			instanceFieldsObjectAppender.beginRow();
			instanceFieldsObjectAppender.append(instanceObjId);
			instanceFieldsObjectAppender.append(fieldNameStringId);
			instanceFieldsObjectAppender.append(value.value != null ? ((Number) value.value).longValue() : 0L);
			instanceFieldsObjectAppender.endRow();
			break;

		case Type.BOOL:
			instanceFieldsBooleanAppender.beginRow();
			instanceFieldsBooleanAppender.append(instanceObjId);
			instanceFieldsBooleanAppender.append(fieldNameStringId);
			instanceFieldsBooleanAppender.append(value.value != null ? (Boolean) value.value : false);
			instanceFieldsBooleanAppender.endRow();
			break;

		case Type.CHAR:
			instanceFieldsCharAppender.beginRow();
			instanceFieldsCharAppender.append(instanceObjId);
			instanceFieldsCharAppender.append(fieldNameStringId);
			instanceFieldsCharAppender.append(value.value != null ? String.valueOf(value.value) : "");
			instanceFieldsCharAppender.endRow();
			break;

		case Type.FLOAT:
			instanceFieldsFloatAppender.beginRow();
			instanceFieldsFloatAppender.append(instanceObjId);
			instanceFieldsFloatAppender.append(fieldNameStringId);
			instanceFieldsFloatAppender.append(value.value != null ? ((Number) value.value).floatValue() : 0.0f);
			instanceFieldsFloatAppender.endRow();
			break;

		case Type.DOUBLE:
			instanceFieldsDoubleAppender.beginRow();
			instanceFieldsDoubleAppender.append(instanceObjId);
			instanceFieldsDoubleAppender.append(fieldNameStringId);
			instanceFieldsDoubleAppender.append(value.value != null ? ((Number) value.value).doubleValue() : 0.0);
			instanceFieldsDoubleAppender.endRow();
			break;

		case Type.BYTE:
			instanceFieldsByteAppender.beginRow();
			instanceFieldsByteAppender.append(instanceObjId);
			instanceFieldsByteAppender.append(fieldNameStringId);
			instanceFieldsByteAppender.append(value.value != null ? ((Number) value.value).byteValue() : (byte) 0);
			instanceFieldsByteAppender.endRow();
			break;

		case Type.SHORT:
			instanceFieldsShortAppender.beginRow();
			instanceFieldsShortAppender.append(instanceObjId);
			instanceFieldsShortAppender.append(fieldNameStringId);
			instanceFieldsShortAppender.append(value.value != null ? ((Number) value.value).shortValue() : (short) 0);
			instanceFieldsShortAppender.endRow();
			break;

		case Type.INT:
			instanceFieldsIntAppender.beginRow();
			instanceFieldsIntAppender.append(instanceObjId);
			instanceFieldsIntAppender.append(fieldNameStringId);
			instanceFieldsIntAppender.append(value.value != null ? ((Number) value.value).intValue() : 0);
			instanceFieldsIntAppender.endRow();
			break;

		case Type.LONG:
			instanceFieldsLongAppender.beginRow();
			instanceFieldsLongAppender.append(instanceObjId);
			instanceFieldsLongAppender.append(fieldNameStringId);
			instanceFieldsLongAppender.append(value.value != null ? ((Number) value.value).longValue() : 0L);
			instanceFieldsLongAppender.endRow();
			break;
		}
		totalRecordsProcessed++;
	}

	@Override
	public void classDump(long classObjId, int stackTraceSerialNum, long superClassObjId, long classLoaderObjId,
			long signersObjId, long protectionDomainObjId, long reserved1, long reserved2, int instanceSize,
			Constant[] constants, Static[] statics, InstanceField[] instanceFields) {
		classInstanceFields.put(classObjId, instanceFields);
		classDumpCount++;
		try {
			// Efficient serialization using thread-local StringBuilder
			StringBuilder sb = stringBuilderCache.get();

			sb.setLength(0);
			if (constants != null) {
				for (int i = 0; i < constants.length; i++) {
					sb.append(constants[i].constantPoolIndex).append(":").append(constants[i].value);
					if (i < constants.length - 1)
						sb.append(",");
				}
			}
			String constantsStr = sb.toString();

			sb.setLength(0);
			if (statics != null) {
				for (int i = 0; i < statics.length; i++) {
					sb.append(statics[i].staticFieldNameStringId).append(":").append(statics[i].value);
					if (i < statics.length - 1)
						sb.append(",");
				}
			}
			String staticsStr = sb.toString();

			sb.setLength(0);
			if (instanceFields != null) {
				for (int i = 0; i < instanceFields.length; i++) {
					sb.append(instanceFields[i].fieldNameStringId).append(":").append(instanceFields[i].type);
					if (i < instanceFields.length - 1)
						sb.append(",");
				}
			}
			String fieldsStr = sb.toString();

			classDumpsAppender.beginRow();
			classDumpsAppender.append(classObjId);
			classDumpsAppender.append(stackTraceSerialNum);
			classDumpsAppender.append(superClassObjId);
			classDumpsAppender.append(classLoaderObjId);
			classDumpsAppender.append(signersObjId);
			classDumpsAppender.append(protectionDomainObjId);
			classDumpsAppender.append(reserved1);
			classDumpsAppender.append(reserved2);
			classDumpsAppender.append(instanceSize);
			classDumpsAppender.append(constantsStr);
			classDumpsAppender.append(staticsStr);
			classDumpsAppender.append(fieldsStr);
			classDumpsAppender.endRow();
			totalRecordsProcessed++;

			if (classDumpCount % 10_000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", classDumpCount) + " class dumps");
			}

			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void objArrayDump(long objId, int stackTraceSerialNum, long elemClassObjId, long[] elems) {
		objArrayCount++;
		try {
			objArraysAppender.beginRow();
			objArraysAppender.append(objId);
			objArraysAppender.append(stackTraceSerialNum);
			objArraysAppender.append(elemClassObjId);

			objArraysAppender.append(elems);

			objArraysAppender.endRow();
			totalRecordsProcessed++;

			if (objArrayCount % 25_000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", objArrayCount) + " object arrays");
			}

			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void primArrayDump(long objId, int stackTraceSerialNum, byte elemType, Value<?>[] elems) {
		primArrayCount++;
		try {
			// Convert Value<?> array to appropriate typed array
			List<?> typedElems = Arrays.stream(elems).map(v -> v.value).collect(Collectors.toList());

			switch (Type.hprofTypeToEnum(elemType)) {
			case Type.BOOL:
				primArraysBooleanAppender.beginRow();
				primArraysBooleanAppender.append(objId);
				primArraysBooleanAppender.append(stackTraceSerialNum);
				primArraysBooleanAppender.append(typedElems);
				primArraysBooleanAppender.endRow();
				break;
			case Type.CHAR:
				String result = typedElems.stream()
						.collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
				primArraysCharAppender.beginRow();
				primArraysCharAppender.append(objId);
				primArraysCharAppender.append(stackTraceSerialNum);
				primArraysCharAppender.append(result);
				primArraysCharAppender.endRow();
				break;
			case Type.FLOAT:
				primArraysFloatAppender.beginRow();
				primArraysFloatAppender.append(objId);
				primArraysFloatAppender.append(stackTraceSerialNum);
				primArraysFloatAppender.append(typedElems);
				primArraysFloatAppender.endRow();
				break;
			case Type.DOUBLE:
				primArraysDoubleAppender.beginRow();
				primArraysDoubleAppender.append(objId);
				primArraysDoubleAppender.append(stackTraceSerialNum);
				primArraysDoubleAppender.append(typedElems);
				primArraysDoubleAppender.endRow();
				break;
			case Type.BYTE:
				primArraysByteAppender.beginRow();
				primArraysByteAppender.append(objId);
				primArraysByteAppender.append(stackTraceSerialNum);
				primArraysByteAppender.append(typedElems);
				primArraysByteAppender.endRow();
				break;
			case Type.SHORT:
				primArraysShortAppender.beginRow();
				primArraysShortAppender.append(objId);
				primArraysShortAppender.append(stackTraceSerialNum);
				primArraysShortAppender.append(typedElems);
				primArraysShortAppender.endRow();
				break;
			case Type.INT:
				primArraysIntAppender.beginRow();
				primArraysIntAppender.append(objId);
				primArraysIntAppender.append(stackTraceSerialNum);
				primArraysIntAppender.append(typedElems);
				primArraysIntAppender.endRow();
				break;
			case Type.LONG:
				primArraysLongAppender.beginRow();
				primArraysLongAppender.append(objId);
				primArraysLongAppender.append(stackTraceSerialNum);
				primArraysLongAppender.append(typedElems);
				primArraysLongAppender.endRow();
				break;
			default:
				throw new Exception("Cannot export prim array type " + String.valueOf(elemType));
			}

			totalRecordsProcessed++;

			if (primArrayCount % 25_000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", primArrayCount) + " primitive arrays");
			}

			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Root operations use per-table appenders created above
	@Override
	public void rootUnknown(long objId) {
		try {
			rootUnknownAppender.beginRow();
			rootUnknownAppender.append(objId);
			rootUnknownAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootJNIGlobal(long objId, long JNIGlobalRefId) {
		try {
			rootJniGlobalAppender.beginRow();
			rootJniGlobalAppender.append(objId);
			rootJniGlobalAppender.append(JNIGlobalRefId);
			rootJniGlobalAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootJNILocal(long objId, int threadSerialNum, int frameNum) {
		try {
			rootJniLocalAppender.beginRow();
			rootJniLocalAppender.append(objId);
			rootJniLocalAppender.append(threadSerialNum);
			rootJniLocalAppender.append(frameNum);
			rootJniLocalAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootJavaFrame(long objId, int threadSerialNum, int frameNum) {
		try {
			rootJavaFrameAppender.beginRow();
			rootJavaFrameAppender.append(objId);
			rootJavaFrameAppender.append(threadSerialNum);
			rootJavaFrameAppender.append(frameNum);
			rootJavaFrameAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootNativeStack(long objId, int threadSerialNum) {
		try {
			rootNativeStackAppender.beginRow();
			rootNativeStackAppender.append(objId);
			rootNativeStackAppender.append(threadSerialNum);
			rootNativeStackAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootStickyClass(long objId) {
		try {
			rootStickyClassAppender.beginRow();
			rootStickyClassAppender.append(objId);
			rootStickyClassAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootThreadBlock(long objId, int threadSerialNum) {
		try {
			rootThreadBlockAppender.beginRow();
			rootThreadBlockAppender.append(objId);
			rootThreadBlockAppender.append(threadSerialNum);
			rootThreadBlockAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootMonitorUsed(long objId) {
		try {
			rootMonitorUsedAppender.beginRow();
			rootMonitorUsedAppender.append(objId);
			rootMonitorUsedAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rootThreadObj(long objId, int threadSerialNum, int stackTraceSerialNum) {
		try {
			rootThreadObjAppender.beginRow();
			rootThreadObjAppender.append(objId);
			rootThreadObjAppender.append(threadSerialNum);
			rootThreadObjAppender.append(stackTraceSerialNum);
			rootThreadObjAppender.endRow();
			totalRecordsProcessed++;
			periodicCommitIfNeeded();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void periodicCommitIfNeeded() {
		try {
			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
				System.out.println(
						"[DuckDB] Progress: " + String.format("%,d", totalRecordsProcessed) + " records committed");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void close() throws Exception {
		System.out.println("[DuckDB] Flushing appenders and finishing export...");

		// close appenders (flush)
		closeAppenderQuiet(stringsAppender);
		closeAppenderQuiet(classesAppender);
		closeAppenderQuiet(threadsAppender);
		closeAppenderQuiet(heapSummaryAppender);
		closeAppenderQuiet(instancesAppender);
		closeAppenderQuiet(instanceFieldsObjectAppender);
		closeAppenderQuiet(instanceFieldsBooleanAppender);
		closeAppenderQuiet(instanceFieldsCharAppender);
		closeAppenderQuiet(instanceFieldsFloatAppender);
		closeAppenderQuiet(instanceFieldsDoubleAppender);
		closeAppenderQuiet(instanceFieldsByteAppender);
		closeAppenderQuiet(instanceFieldsShortAppender);
		closeAppenderQuiet(instanceFieldsIntAppender);
		closeAppenderQuiet(instanceFieldsLongAppender);
		closeAppenderQuiet(classDumpsAppender);
		closeAppenderQuiet(objArraysAppender);

		// Close primitive array appenders
		closeAppenderQuiet(primArraysBooleanAppender);
		closeAppenderQuiet(primArraysCharAppender);
		closeAppenderQuiet(primArraysFloatAppender);
		closeAppenderQuiet(primArraysDoubleAppender);
		closeAppenderQuiet(primArraysByteAppender);
		closeAppenderQuiet(primArraysShortAppender);
		closeAppenderQuiet(primArraysIntAppender);
		closeAppenderQuiet(primArraysLongAppender);

		closeAppenderQuiet(rootUnknownAppender);
		closeAppenderQuiet(rootJniGlobalAppender);
		closeAppenderQuiet(rootJniLocalAppender);
		closeAppenderQuiet(rootJavaFrameAppender);
		closeAppenderQuiet(rootNativeStackAppender);
		closeAppenderQuiet(rootStickyClassAppender);
		closeAppenderQuiet(rootThreadBlockAppender);
		closeAppenderQuiet(rootMonitorUsedAppender);
		closeAppenderQuiet(rootThreadObjAppender);

		// Final commit
		try {
			conn.commit();
		} catch (SQLException e) {
			System.err.println("[DuckDB] Error committing: " + e.getMessage());
		}

		// Create indexes for query performance
		System.out.println("[DuckDB] Creating indexes...");
		try (Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_strings_id ON strings(id)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_classes_objid ON classes(classObjId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_instances_objid ON instances(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_instances_classid ON instances(classObjId)");

			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_object_objid ON instance_fields_object(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_boolean_objid ON instance_fields_boolean(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_char_objid ON instance_fields_char(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_float_objid ON instance_fields_float(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_double_objid ON instance_fields_double(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_byte_objid ON instance_fields_byte(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_short_objid ON instance_fields_short(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_int_objid ON instance_fields_int(instanceObjId)");
			stmt.execute(
					"CREATE INDEX IF NOT EXISTS idx_instance_fields_long_objid ON instance_fields_long(instanceObjId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_class_dumps_objid ON class_dumps(classObjId)");

			// Indexes for primitive array tables
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_boolean_objid ON prim_arrays_boolean(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_char_objid ON prim_arrays_char(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_float_objid ON prim_arrays_float(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_double_objid ON prim_arrays_double(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_byte_objid ON prim_arrays_byte(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_short_objid ON prim_arrays_short(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_int_objid ON prim_arrays_int(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_prim_arrays_long_objid ON prim_arrays_long(objId)");

			conn.commit();
		} catch (SQLException e) {
			System.err.println("[DuckDB] Error creating indexes: " + e.getMessage());
		}

		// Re-enable compression and optimize database
		System.out.println("[DuckDB] Optimizing database file...");
		try (Statement stmt = conn.createStatement()) {
			stmt.execute("PRAGMA force_compression='auto'");
			stmt.execute("CHECKPOINT");
		} catch (SQLException e) {
			System.err.println("[DuckDB] Error optimizing database: " + e.getMessage());
		}

		// Final commit and close
		try {
			if (conn != null && !conn.isClosed()) {
				conn.commit();
				conn.close();
			}
		} catch (SQLException e) {
			System.err.println("[DuckDB] Error closing connection: " + e.getMessage());
		}

		System.out.println("\n[DuckDB] Export Complete");
		System.out.println("========================================");
		System.out.println("  Strings:          " + String.format("%,d", stringCount));
		System.out.println("  Classes:          " + String.format("%,d", classCount));
		System.out.println("  Instances:        " + String.format("%,d", instanceCount));
		System.out.println("  Class Dumps:      " + String.format("%,d", classDumpCount));
		System.out.println("  Object Arrays:    " + String.format("%,d", objArrayCount));
		System.out.println("  Primitive Arrays: " + String.format("%,d", primArrayCount));
		System.out.println("  Total Records:    " + String.format("%,d", totalRecordsProcessed));
		System.out.println("========================================");
		System.out.println("  Database: " + dbPath);
		System.out.println("  Status: Optimized and indexed");
	}

	private void closeAppenderQuiet(DuckDBAppender app) {
		if (app == null)
			return;
		try {
			app.close();
		} catch (Exception e) {
			System.err.println("[DuckDB] Error closing appender: " + e.getMessage());
		}
	}

	private void closeQuiet() {
		try {
			if (conn != null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			// ignore
		}
	}
}
