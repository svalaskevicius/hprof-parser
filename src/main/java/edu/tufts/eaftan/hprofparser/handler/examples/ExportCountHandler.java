package edu.tufts.eaftan.hprofparser.handler.examples;

import edu.tufts.eaftan.hprofparser.handler.NullRecordHandler;
import edu.tufts.eaftan.hprofparser.parser.datastructures.AllocSite;
import edu.tufts.eaftan.hprofparser.parser.datastructures.CPUSample;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Constant;
import edu.tufts.eaftan.hprofparser.parser.datastructures.InstanceField;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Static;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Value;

import java.util.HashMap;

/**
 * Prints counts for each export type encountered.
 */
public class ExportCountHandler extends NullRecordHandler {

  // Counters for each type of record
  private int stringCount = 0;
  private int classCount = 0;
  private int loadClassCount = 0;
  private int unloadClassCount = 0;
  private int stackFrameCount = 0;
  private int stackTraceCount = 0;
  private int allocSitesCount = 0;
  private int heapSummaryCount = 0;
  private int startThreadCount = 0;
  private int endThreadCount = 0;
  private int heapDumpCount = 0;
  private int heapDumpEndCount = 0;
  private int heapDumpSegmentCount = 0;
  private int cpuSamplesCount = 0;
  private int controlSettingsCount = 0;
  private int rootUnknownCount = 0;
  private int rootJNIGlobalCount = 0;
  private int rootJNILocalCount = 0;
  private int rootJavaFrameCount = 0;
  private int rootNativeStackCount = 0;
  private int rootStickyClassCount = 0;
  private int rootThreadBlockCount = 0;
  private int rootMonitorUsedCount = 0;
  private int rootThreadObjCount = 0;
  private int classDumpCount = 0;
  private int instanceDumpCount = 0;
  private int objArrayDumpCount = 0;
  private int primArrayDumpCount = 0;

  @Override
  public void header(String format, int idSize, long time) {
    // Header processing - no counting needed
  }

  @Override
  public void stringInUTF8(long id, String data) {
    stringCount++;
  }

  @Override
  public void loadClass(int classSerialNum, long classObjId, 
      int stackTraceSerialNum, long classNameStringId) {
    loadClassCount++;
  }

  @Override
  public void unloadClass(int classSerialNum) {
    unloadClassCount++;
  }

  @Override
  public void stackFrame(long stackFrameId, long methodNameStringId, 
      long methodSigStringId, long sourceFileNameStringId, 
      int classSerialNum, int location) {
    stackFrameCount++;
  }

  @Override
  public void stackTrace(int stackTraceSerialNum, int threadSerialNum, 
      int numFrames, long[] stackFrameIds) {
    stackTraceCount++;
  }

  @Override
  public void allocSites(short bitMaskFlags, float cutoffRatio, 
      int totalLiveBytes, int totalLiveInstances, long totalBytesAllocated,
      long totalInstancesAllocated, AllocSite[] sites) {
    allocSitesCount++;
  }

  @Override
  public void heapSummary(int totalLiveBytes, int totalLiveInstances,
      long totalBytesAllocated, long totalInstancesAllocated) {
    heapSummaryCount++;
  }

  @Override
  public void startThread(int threadSerialNum, long threadObjectId,
      int stackTraceSerialNum, long threadNameStringId, long threadGroupNameId,
      long threadParentGroupNameId) {
    startThreadCount++;
  }

  @Override
  public void endThread(int threadSerialNum) {
    endThreadCount++;
  }

  @Override
  public void heapDump() {
    heapDumpCount++;
  }
  
  @Override
  public void heapDumpEnd() {
    heapDumpEndCount++;
  }

  @Override
  public void heapDumpSegment() {
    heapDumpSegmentCount++;
  }

  @Override
  public void cpuSamples(int totalNumOfSamples, CPUSample[] samples) {
    cpuSamplesCount++;
  }

  @Override
  public void controlSettings(int bitMaskFlags, short stackTraceDepth) {
    controlSettingsCount++;
  }

  @Override
  public void rootUnknown(long objId) {
    rootUnknownCount++;
  }

  @Override
  public void rootJNIGlobal(long objId, long JNIGlobalRefId) {
    rootJNIGlobalCount++;
  }

  @Override
  public void rootJNILocal(long objId, int threadSerialNum, int frameNum) {
    rootJNILocalCount++;
  }

  @Override
  public void rootJavaFrame(long objId, int threadSerialNum, int frameNum) {
    rootJavaFrameCount++;
  }

  @Override
  public void rootNativeStack(long objId, int threadSerialNum) {
    rootNativeStackCount++;
  }

  @Override
  public void rootStickyClass(long objId) {
    rootStickyClassCount++;
  }

  @Override
  public void rootThreadBlock(long objId, int threadSerialNum) {
    rootThreadBlockCount++;
  }

  @Override
  public void rootMonitorUsed(long objId) {
    rootMonitorUsedCount++;
  }

  @Override
  public void rootThreadObj(long objId, int threadSerialNum, 
      int stackTraceSerialNum) {
    rootThreadObjCount++;
  }

  @Override
  public void classDump(long classObjId, int stackTraceSerialNum, 
      long superClassObjId, long classLoaderObjId, long signersObjId,
      long protectionDomainObjId, long reserved1, long reserved2, 
      int instanceSize, Constant[] constants, Static[] statics,
      InstanceField[] instanceFields) {
    classDumpCount++;
  }

  @Override
  public void instanceDump(long objId, int stackTraceSerialNum, 
      long classObjId, Value<?>[] instanceFieldValues) {
    instanceDumpCount++;
  }

  @Override
  public void objArrayDump(long objId, int stackTraceSerialNum, 
      long elemClassObjId, long[] elems) {
    objArrayDumpCount++;
  }

  @Override
  public void primArrayDump(long objId, int stackTraceSerialNum, 
      byte elemType, Value<?>[] elems) {
    primArrayDumpCount++;
  }

  /**
   * Prints the final counts of each export type.
   */
  public void printCounts() {
    System.out.println("Export Type Counts:");
    System.out.println("===================");
    System.out.println("Strings: " + stringCount);
    System.out.println("Classes: " + classCount);
    System.out.println("Load Class: " + loadClassCount);
    System.out.println("Unload Class: " + unloadClassCount);
    System.out.println("Stack Frames: " + stackFrameCount);
    System.out.println("Stack Traces: " + stackTraceCount);
    System.out.println("Alloc Sites: " + allocSitesCount);
    System.out.println("Heap Summary: " + heapSummaryCount);
    System.out.println("Start Thread: " + startThreadCount);
    System.out.println("End Thread: " + endThreadCount);
    System.out.println("Heap Dump: " + heapDumpCount);
    System.out.println("Heap Dump End: " + heapDumpEndCount);
    System.out.println("Heap Dump Segment: " + heapDumpSegmentCount);
    System.out.println("CPU Samples: " + cpuSamplesCount);
    System.out.println("Control Settings: " + controlSettingsCount);
    System.out.println("Root Unknown: " + rootUnknownCount);
    System.out.println("Root JNI Global: " + rootJNIGlobalCount);
    System.out.println("Root JNI Local: " + rootJNILocalCount);
    System.out.println("Root Java Frame: " + rootJavaFrameCount);
    System.out.println("Root Native Stack: " + rootNativeStackCount);
    System.out.println("Root Sticky Class: " + rootStickyClassCount);
    System.out.println("Root Thread Block: " + rootThreadBlockCount);
    System.out.println("Root Monitor Used: " + rootMonitorUsedCount);
    System.out.println("Root Thread Object: " + rootThreadObjCount);
    System.out.println("Class Dump: " + classDumpCount);
    System.out.println("Instance Dump: " + instanceDumpCount);
    System.out.println("Object Array Dump: " + objArrayDumpCount);
    System.out.println("Primitive Array Dump: " + primArrayDumpCount);
    System.out.println("===================");
  }
}
