package edu.tufts.eaftan.hprofparser.handler.examples;

import edu.tufts.eaftan.hprofparser.handler.NullRecordHandler;
import edu.tufts.eaftan.hprofparser.parser.datastructures.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

public class DuckDbExportHandler extends NullRecordHandler {

	private Connection conn;
	private String dbPath;

	// Performance settings
	private static final int BATCH_SIZE = 10000; // Larger batches
	private static final int COMMIT_INTERVAL = 100000; // Commit every 100K records

	// Map from classObjId to its instance fields
	private Map<Long, InstanceField[]> classInstanceFields = new ConcurrentHashMap<>();
	private Map<Long, String> stringIdToValue = new ConcurrentHashMap<>();

	// Prepared statements with reuse
	private PreparedStatement stringStmt;
	private PreparedStatement classStmt;
	private PreparedStatement instanceStmt;
	private PreparedStatement instanceFieldStmt;
	private PreparedStatement classDumpStmt;
	private PreparedStatement objArrayStmt;
	private PreparedStatement primArrayStmt;

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
	private long totalRecordsProcessed = 0;

	// Thread pool for parallel flushing
	private ExecutorService flushExecutor;
	private Semaphore flushSemaphore = new Semaphore(3); // Limit concurrent flushes

	// Reusable objects
	private ThreadLocal<StringBuilder> stringBuilderCache = ThreadLocal.withInitial(() -> new StringBuilder(8192));

	public DuckDbExportHandler(String dbPath) throws SQLException {
		this.dbPath = dbPath;
		this.flushExecutor = Executors.newFixedThreadPool(5); // Parallel flush threads

		// Connect to DuckDB
		conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
		conn.setAutoCommit(false);

		// Performance tuning for DuckDB
		Statement stmt = conn.createStatement();
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
				"CREATE TABLE IF NOT EXISTS instance_fields (instanceObjId BIGINT, fieldName VARCHAR, fieldType VARCHAR, fieldValue VARCHAR)");
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS class_dumps (classObjId BIGINT, stackTraceSerialNum INT, superClassObjId BIGINT, classLoaderObjId BIGINT, signersObjId BIGINT, protectionDomainObjId BIGINT, reserved1 BIGINT, reserved2 BIGINT, instanceSize INT, constants VARCHAR, statics VARCHAR, instanceFields VARCHAR)");
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS obj_arrays (objId BIGINT, stackTraceSerialNum INT, elemClassObjId BIGINT, elems BIGINT[])");
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS prim_arrays (objId BIGINT, stackTraceSerialNum INT, elemType SMALLINT, elems VARCHAR[])");

		// Root tables
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_unknown (col0 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_jni_global (col0 BIGINT, col1 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_jni_local (col0 BIGINT, col1 BIGINT, col2 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_java_frame (col0 BIGINT, col1 BIGINT, col2 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_native_stack (col0 BIGINT, col1 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_sticky_class (col0 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_thread_block (col0 BIGINT, col1 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_monitor_used (col0 BIGINT)");
		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS root_thread_obj (col0 BIGINT, col1 BIGINT, col2 BIGINT)");

		conn.commit();

		// Prepare all statements for reuse
		stringStmt = conn.prepareStatement("INSERT INTO strings VALUES (?, ?)");
		classStmt = conn.prepareStatement("INSERT INTO classes VALUES (?, ?, ?, ?)");
		instanceStmt = conn.prepareStatement("INSERT INTO instances VALUES (?, ?, ?)");
		instanceFieldStmt = conn.prepareStatement("INSERT INTO instance_fields VALUES (?, ?, ?, ?)");
		classDumpStmt = conn.prepareStatement("INSERT INTO class_dumps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		objArrayStmt = conn.prepareStatement("INSERT INTO obj_arrays VALUES (?, ?, ?, ?)");
		primArrayStmt = conn.prepareStatement("INSERT INTO prim_arrays VALUES (?, ?, ?, ?)");

		System.out.println("[DuckDB] Database initialized with performance optimizations");
	}

	@Override
	public void header(String format, int idSize, long time) {
		// Header processing
	}

	@Override
	public void stringInUTF8(long id, String data) {
		stringCount++;
		stringIdToValue.put(id, data);

		try {
			// Check if statement is still open, recreate if necessary
			if (stringStmt == null || stringStmt.isClosed()) {
				stringStmt = conn.prepareStatement("INSERT INTO strings VALUES (?, ?)");
				stringBatchCount = 0;
			}

			stringStmt.setLong(1, id);
			stringStmt.setString(2, data);
			stringStmt.addBatch();
			stringBatchCount++;
			totalRecordsProcessed++;

			if (stringBatchCount >= BATCH_SIZE) {
				stringStmt.executeBatch();
				stringStmt.close();
				stringStmt = null;
				stringBatchCount = 0;
			}

			if (stringCount % 50000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", stringCount) + " strings");
			}

			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
				System.out.println(
						"[DuckDB] Progress: " + String.format("%,d", totalRecordsProcessed) + " records committed");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void loadClass(int classSerialNum, long classObjId, int stackTraceSerialNum, long classNameStringId) {
		classCount++;

		try {
			if (classStmt == null || classStmt.isClosed()) {
				classStmt = conn.prepareStatement("INSERT INTO classes VALUES (?, ?, ?, ?)");
				classBatchCount = 0;
			}

			classStmt.setInt(1, classSerialNum);
			classStmt.setLong(2, classObjId);
			classStmt.setInt(3, stackTraceSerialNum);
			classStmt.setLong(4, classNameStringId);
			classStmt.addBatch();
			classBatchCount++;
			totalRecordsProcessed++;

			if (classBatchCount >= BATCH_SIZE) {
				classStmt.executeBatch();
				classStmt.close();
				classStmt = null;
				classBatchCount = 0;
			}

			if (classCount % 10000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", classCount) + " classes");
			}

			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startThread(int threadSerialNum, long threadObjectId, int stackTraceSerialNum, long threadNameStringId,
			long threadGroupNameId, long threadParentGroupNameId) {
		try (PreparedStatement ps = conn.prepareStatement("INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?)")) {
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
		try (PreparedStatement ps = conn.prepareStatement("INSERT INTO heap_summary VALUES (?, ?, ?, ?)")) {
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
			if (instanceStmt == null || instanceStmt.isClosed()) {
				instanceStmt = conn.prepareStatement("INSERT INTO instances VALUES (?, ?, ?)");
				instanceBatchCount = 0;
			}

			instanceStmt.setLong(1, objId);
			instanceStmt.setInt(2, stackTraceSerialNum);
			instanceStmt.setLong(3, classObjId);
			instanceStmt.addBatch();
			instanceBatchCount++;
			totalRecordsProcessed++;

			if (instanceBatchCount >= BATCH_SIZE) {
				instanceStmt.executeBatch();
				instanceStmt.close();
				instanceStmt = null;
				instanceBatchCount = 0;
			}

			// Insert normalized instance fields
			InstanceField[] fields = classInstanceFields.get(classObjId);
			if (fields != null && instanceFieldValues != null && fields.length == instanceFieldValues.length) {
				if (instanceFieldStmt == null || instanceFieldStmt.isClosed()) {
					instanceFieldStmt = conn.prepareStatement("INSERT INTO instance_fields VALUES (?, ?, ?, ?)");
					instanceFieldBatchCount = 0;
				}

				for (int i = 0; i < fields.length; i++) {
					instanceFieldStmt.setLong(1, objId);
					instanceFieldStmt.setString(2, stringIdToValue.getOrDefault(fields[i].fieldNameStringId,
							String.valueOf(fields[i].fieldNameStringId)));
					instanceFieldStmt.setString(3, fields[i].type.toString());
					instanceFieldStmt.setString(4, String.valueOf(instanceFieldValues[i]));
					instanceFieldStmt.addBatch();
					instanceFieldBatchCount++;

					if (instanceFieldBatchCount >= BATCH_SIZE) {
						instanceFieldStmt.executeBatch();
						instanceFieldStmt.close();
						instanceFieldStmt = null;
						instanceFieldBatchCount = 0;
					}
				}
			}

			if (instanceCount % 50000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", instanceCount) + " instances");
			}

			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
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
			if (classDumpStmt == null || classDumpStmt.isClosed()) {
				classDumpStmt = conn
						.prepareStatement("INSERT INTO class_dumps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				classDumpBatchCount = 0;
			}

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

			classDumpStmt.setLong(1, classObjId);
			classDumpStmt.setInt(2, stackTraceSerialNum);
			classDumpStmt.setLong(3, superClassObjId);
			classDumpStmt.setLong(4, classLoaderObjId);
			classDumpStmt.setLong(5, signersObjId);
			classDumpStmt.setLong(6, protectionDomainObjId);
			classDumpStmt.setLong(7, reserved1);
			classDumpStmt.setLong(8, reserved2);
			classDumpStmt.setInt(9, instanceSize);
			classDumpStmt.setString(10, constantsStr);
			classDumpStmt.setString(11, staticsStr);
			classDumpStmt.setString(12, fieldsStr);
			classDumpStmt.addBatch();
			classDumpBatchCount++;
			totalRecordsProcessed++;

			if (classDumpBatchCount >= BATCH_SIZE) {
				classDumpStmt.executeBatch();
				classDumpStmt.close();
				classDumpStmt = null;
				classDumpBatchCount = 0;
			}

			if (classDumpCount % 10000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", classDumpCount) + " class dumps");
			}

			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void objArrayDump(long objId, int stackTraceSerialNum, long elemClassObjId, long[] elems) {
		objArrayCount++;

		try {
			if (objArrayStmt == null || objArrayStmt.isClosed()) {
				objArrayStmt = conn.prepareStatement("INSERT INTO obj_arrays VALUES (?, ?, ?, ?)");
				objArrayBatchCount = 0;
			}

			objArrayStmt.setLong(1, objId);
			objArrayStmt.setInt(2, stackTraceSerialNum);
			objArrayStmt.setLong(3, elemClassObjId);

			// Set as SQL array type - DuckDB will store as BIGINT LIST
			if (elems != null && elems.length > 0) {
				// Convert long[] to Long[] for createArrayOf
				Long[] elemObjects = new Long[elems.length];
				for (int i = 0; i < elems.length; i++) {
					elemObjects[i] = elems[i];
				}
				java.sql.Array sqlArray = conn.createArrayOf("BIGINT", elemObjects);
				objArrayStmt.setArray(4, sqlArray);
			} else {
				objArrayStmt.setNull(4, java.sql.Types.ARRAY);
			}

			objArrayStmt.addBatch();
			objArrayBatchCount++;
			totalRecordsProcessed++;

			if (objArrayBatchCount >= BATCH_SIZE) {
				objArrayStmt.executeBatch();
				objArrayStmt.close();
				objArrayStmt = null;
				objArrayBatchCount = 0;
			}

			if (objArrayCount % 25000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", objArrayCount) + " object arrays");
			}

			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void primArrayDump(long objId, int stackTraceSerialNum, byte elemType, Value<?>[] elems) {
		primArrayCount++;

		try {
			if (primArrayStmt == null || primArrayStmt.isClosed()) {
				primArrayStmt = conn.prepareStatement("INSERT INTO prim_arrays VALUES (?, ?, ?, ?)");
				primArrayBatchCount = 0;
			}

			// Convert array to DuckDB LIST type (VARCHAR[])
			String[] elemStrings = null;
			if (elems != null && elems.length > 0) {
				elemStrings = new String[elems.length];
				for (int i = 0; i < elems.length; i++) {
					elemStrings[i] = String.valueOf(elems[i]);
				}
			}

			primArrayStmt.setLong(1, objId);
			primArrayStmt.setInt(2, stackTraceSerialNum);
			primArrayStmt.setShort(3, elemType);

			// Set as SQL array type - DuckDB will store as LIST
			if (elemStrings != null) {
				java.sql.Array sqlArray = conn.createArrayOf("VARCHAR", elemStrings);
				primArrayStmt.setArray(4, sqlArray);
			} else {
				primArrayStmt.setNull(4, java.sql.Types.ARRAY);
			}

			primArrayStmt.addBatch();
			primArrayBatchCount++;
			totalRecordsProcessed++;

			if (primArrayBatchCount >= BATCH_SIZE) {
				primArrayStmt.executeBatch();
				primArrayStmt.close();
				primArrayStmt = null;
				primArrayBatchCount = 0;
			}

			if (primArrayCount % 25000 == 0) {
				System.out.println("[DuckDB] Processed " + String.format("%,d", primArrayCount) + " primitive arrays");
			}

			if (totalRecordsProcessed % COMMIT_INTERVAL == 0) {
				conn.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Root operations
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

	private void exportRoot(String table, Object... values) {
		try {
			StringBuilder insert = new StringBuilder("INSERT INTO ").append(table).append(" VALUES (");
			for (int i = 0; i < values.length; i++) {
				insert.append("?");
				if (i < values.length - 1)
					insert.append(", ");
			}
			insert.append(")");

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

	public void close() throws Exception {
		System.out.println("[DuckDB] Flushing remaining batches...");

		// Flush all remaining batches
		try {
			if (stringBatchCount > 0 && stringStmt != null && !stringStmt.isClosed()) {
				stringStmt.executeBatch();
			}
			if (classBatchCount > 0 && classStmt != null && !classStmt.isClosed()) {
				classStmt.executeBatch();
			}
			if (instanceBatchCount > 0 && instanceStmt != null && !instanceStmt.isClosed()) {
				instanceStmt.executeBatch();
			}
			if (instanceFieldBatchCount > 0 && instanceFieldStmt != null && !instanceFieldStmt.isClosed()) {
				instanceFieldStmt.executeBatch();
			}
			if (classDumpBatchCount > 0 && classDumpStmt != null && !classDumpStmt.isClosed()) {
				classDumpStmt.executeBatch();
			}
			if (objArrayBatchCount > 0 && objArrayStmt != null && !objArrayStmt.isClosed()) {
				objArrayStmt.executeBatch();
			}
			if (primArrayBatchCount > 0 && primArrayStmt != null && !primArrayStmt.isClosed()) {
				primArrayStmt.executeBatch();
			}

			conn.commit();
		} catch (SQLException e) {
			System.err.println("[DuckDB] Error flushing batches: " + e.getMessage());
			e.printStackTrace();
		}

		// Close prepared statements
		try {
			if (stringStmt != null && !stringStmt.isClosed())
				stringStmt.close();
			if (classStmt != null && !classStmt.isClosed())
				classStmt.close();
			if (instanceStmt != null && !instanceStmt.isClosed())
				instanceStmt.close();
			if (instanceFieldStmt != null && !instanceFieldStmt.isClosed())
				instanceFieldStmt.close();
			if (classDumpStmt != null && !classDumpStmt.isClosed())
				classDumpStmt.close();
			if (objArrayStmt != null && !objArrayStmt.isClosed())
				objArrayStmt.close();
			if (primArrayStmt != null && !primArrayStmt.isClosed())
				primArrayStmt.close();
		} catch (SQLException e) {
			System.err.println("[DuckDB] Error closing statements: " + e.getMessage());
		}

		// Shutdown executor
		flushExecutor.shutdown();
		flushExecutor.awaitTermination(1, TimeUnit.MINUTES);

		// Create indexes for query performance
		System.out.println("[DuckDB] Creating indexes...");
		try (Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_strings_id ON strings(id)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_classes_objid ON classes(classObjId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_instances_objid ON instances(objId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_instances_classid ON instances(classObjId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_instance_fields_objid ON instance_fields(instanceObjId)");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_class_dumps_objid ON class_dumps(classObjId)");
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
}
