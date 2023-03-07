/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.natraft.protocol.log.manager;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.natraft.exception.LogExecutionException;
import org.apache.iotdb.consensus.natraft.protocol.HardState;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.LogApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.EmptyEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization.LogManagerMeta;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization.StableEntryManager;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.Snapshot;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public abstract class RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

  protected RaftConfig config;

  /** manage uncommitted entries */
  private List<Entry> entries;

  /** manage committed entries in disk for safety */
  private StableEntryManager stableEntryManager;

  private volatile long commitIndex;

  /**
   * The committed logs whose index is smaller than this are all have been applied, for example,
   * suppose there are 5 committed logs, whose log index is 1,2,3,4,5; if the applied sequence is
   * 1,3,2,5,4, then the maxHaveAppliedCommitIndex according is 1,1,3,3,5. This attributed is only
   * used for asyncLogApplier
   */
  private volatile long appliedIndex;

  private volatile long appliedTerm;

  private final Object changeApplyCommitIndexCond = new Object();

  protected LogApplier logApplier;

  /** to distinguish managers of different members */
  private String name;

  private ScheduledExecutorService deleteLogExecutorService;
  private ScheduledFuture<?> deleteLogFuture;

  private ExecutorService checkLogApplierExecutorService;
  private Future<?> checkLogApplierFuture;

  /** minimum number of committed logs in memory */
  private int minNumOfLogsInMem;

  /** maximum number of committed logs in memory */
  private int maxNumOfLogsInMem;

  private long maxLogMemSize;

  /**
   * Each time new logs are appended, this condition will be notified so logs that have larger
   * indices but arrived earlier can proceed.
   */
  private final Object[] logUpdateConditions = new Object[1024];

  protected List<Entry> blockedUnappliedLogList;

  protected IStateMachine stateMachine;

  protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private long committedEntrySize;

  protected RaftLogManager(
      StableEntryManager stableEntryManager,
      LogApplier applier,
      String name,
      IStateMachine stateMachine,
      RaftConfig config,
      Consumer<List<Entry>> unappliedEntryExaminer) {
    this.logApplier = applier;
    this.name = name;
    this.stateMachine = stateMachine;
    this.setStableEntryManager(stableEntryManager);
    this.config = config;

    initConf();
    initEntries(unappliedEntryExaminer);

    this.blockedUnappliedLogList = new CopyOnWriteArrayList<>();

    this.deleteLogExecutorService =
        IoTDBThreadPoolFactory.newScheduledThreadPoolWithDaemon(1, "raft-log-delete-" + name);

    this.checkLogApplierExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutorWithDaemon("check-log-applier-" + name);

    /** deletion check period of the submitted log */
    int logDeleteCheckIntervalSecond = config.getLogDeleteCheckIntervalSecond();

    if (logDeleteCheckIntervalSecond > 0) {
      this.deleteLogFuture =
          ScheduledExecutorUtil.safelyScheduleAtFixedRate(
              deleteLogExecutorService,
              this::checkDeleteLog,
              logDeleteCheckIntervalSecond,
              logDeleteCheckIntervalSecond,
              TimeUnit.SECONDS);
    }

    this.checkLogApplierFuture = checkLogApplierExecutorService.submit(this::checkAppliedLogIndex);

    /** flush log to file periodically */
    if (config.isEnableRaftLogPersistence()) {
      this.applyAllCommittedLogWhenStartUp();
    }

    for (int i = 0; i < logUpdateConditions.length; i++) {
      logUpdateConditions[i] = new Object();
    }
  }

  private void initEntries(Consumer<List<Entry>> unappliedEntryExaminer) {
    LogManagerMeta meta = stableEntryManager.getMeta();
    List<Entry> allEntriesAfterAppliedIndex = stableEntryManager.getAllEntriesAfterAppliedIndex();
    unappliedEntryExaminer.accept(allEntriesAfterAppliedIndex);

    entries = new ArrayList<>();
    if (!allEntriesAfterAppliedIndex.isEmpty()) {
      entries.addAll(allEntriesAfterAppliedIndex);
    } else {
      entries.add(
          new EmptyEntry(
              meta.getLastAppliedIndex() == -1 ? -1 : meta.getLastAppliedIndex() - 1,
              meta.getLastAppliedTerm()));
    }

    this.commitIndex = meta.getCommitLogIndex();
    this.appliedIndex = meta.getLastAppliedIndex();
    this.appliedTerm = meta.getLastAppliedTerm();

    for (Entry entry : entries) {
      if (entry.getCurrLogIndex() <= commitIndex) {
        committedEntrySize += entry.estimateSize();
      }
    }
  }

  private void initConf() {
    minNumOfLogsInMem = config.getMinNumOfLogsInMem();
    maxNumOfLogsInMem = config.getMaxNumOfLogsInMem();
    maxLogMemSize = config.getMaxMemorySizeForRaftLog();
  }

  public Snapshot getSnapshot() {
    return getSnapshot(-1);
  }

  public abstract Snapshot getSnapshot(long minLogIndex);

  /**
   * IMPORTANT!!!
   *
   * <p>The subclass's takeSnapshot() must call this method to insure that all logs have been
   * applied before take snapshot
   *
   * <p>
   *
   * @throws IOException timeout exception
   */
  public abstract void takeSnapshot(RaftMember member);

  /**
   * Update the raftNode's hardState(currentTerm,voteFor) and flush to disk.
   *
   * @param state
   */
  public void updateHardState(HardState state) {
    getStableEntryManager().setHardStateAndFlush(state);
  }

  /**
   * Return the raftNode's hardState(currentTerm,voteFor).
   *
   * @return state
   */
  public HardState getHardState() {
    return getStableEntryManager().getHardState();
  }

  /**
   * Return the raftNode's commitIndex.
   *
   * @return commitIndex
   */
  public long getCommitLogIndex() {
    return commitIndex;
  }

  /**
   * Return the first entry's index which have not been compacted.
   *
   * @return firstIndex
   */
  public long getFirstIndex() {
    return entries.get(0).getCurrLogIndex();
  }

  /**
   * Return the last entry's index which have been added into log module.
   *
   * @return lastIndex
   */
  public long getLastLogIndex() {
    try {
      lock.readLock().lock();
      return entries.get(entries.size() - 1).getCurrLogIndex();
    } finally {
      lock.readLock().unlock();
    }
  }

  public Entry getLastEntry() {
    try {
      lock.readLock().lock();
      return entries.get(entries.size() - 1);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the term for given index.
   *
   * @param index request entry index
   * @return throw EntryCompactedException if index < dummyIndex, -1 if index > lastIndex or the
   *     entry is compacted, otherwise return the entry's term for given index
   */
  public long getTerm(long index) {
    try {
      lock.readLock().lock();
      long firstIndex = getFirstIndex();
      if (index < firstIndex) {
        // search in disk
        if (config.isEnableRaftLogPersistence()) {
          List<Entry> logsInDisk = getStableEntryManager().getEntries(index, index);
          if (logsInDisk.isEmpty()) {
            return -1;
          } else {
            return logsInDisk.get(0).getCurrLogTerm();
          }
        }
        return -1;
      }

      long lastIndex = getLastLogIndex();
      if (index > lastIndex) {
        return -1;
      }

      firstIndex = getFirstIndex();
      return entries.get((int) (index - firstIndex)).getCurrLogTerm();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return the last entry's term. If it goes wrong, there must be an unexpected exception.
   *
   * @return last entry's term
   */
  public long getLastLogTerm() {
    try {
      lock.readLock().lock();
      return entries.get(entries.size() - 1).getCurrLogTerm();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return the commitIndex's term. If it goes wrong, there must be an unexpected exception.
   *
   * @return commitIndex's term
   */
  public long getCommitLogTerm() {
    return getTerm(commitIndex);
  }

  /**
   * Used by follower node to support leader's complicated log replication rpc parameters and try to
   * commit entries.
   *
   * @param lastIndex leader's matchIndex for this follower node
   * @param lastTerm the entry's term which index is leader's matchIndex for this follower node
   * @param entries entries sent from the leader node Note that the leader must ensure
   *     entries[0].index = lastIndex + 1
   * @return -1 if the entries cannot be appended, otherwise the last index of new entries
   */
  public long maybeAppend(long lastIndex, long lastTerm, List<Entry> entries) {
    try {
      lock.writeLock().lock();
      if (matchTerm(lastTerm, lastIndex)) {
        long newLastIndex = lastIndex + entries.size();
        long ci = findConflict(entries);
        if (ci <= commitIndex) {
          if (ci != -1) {
            logger.error(
                "{}: entry {} conflict with committed entry [commitIndex({})]",
                name,
                ci,
                commitIndex);
          } else {
            if (logger.isDebugEnabled() && !entries.isEmpty()) {
              logger.debug(
                  "{}: Appending entries [{} and other {} logs] all exist locally",
                  name,
                  entries.get(0),
                  entries.size() - 1);
            }
          }

        } else {
          long offset = lastIndex + 1;
          append(entries.subList((int) (ci - offset), entries.size()));
        }
        return newLastIndex;
      }
      return -1;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Used by leader node or MaybeAppend to directly append to unCommittedEntryManager. Note that the
   * caller should ensure entries[0].index > committed.
   *
   * @param appendingEntries appendingEntries
   * @return the newly generated lastIndex
   */
  public long append(List<Entry> appendingEntries) {
    if (entries.isEmpty()) {
      return getLastLogIndex();
    }

    long after = appendingEntries.get(0).getCurrLogIndex();
    long len = after - getFirstIndex();
    if (len < 0) {
      // the logs are being truncated to before our current offset portion, which is committed
      // entries
      logger.error("The logs which first index is {} are going to truncate committed logs", after);
    } else if (len == entries.size()) {
      // after is the next index in the entries
      // directly append
      entries.addAll(appendingEntries);
    } else {
      // clear conflict entries
      // then append
      logger.info("truncate the entries after index {}", after);
      int truncateIndex = (int) (after - getFirstIndex());
      if (truncateIndex < entries.size()) {
        entries.subList(truncateIndex, entries.size()).clear();
      }
      entries.addAll(appendingEntries);
    }

    Object logUpdateCondition =
        getLogUpdateCondition(entries.get(entries.size() - 1).getCurrLogIndex());
    synchronized (logUpdateCondition) {
      logUpdateCondition.notifyAll();
    }
    return getLastLogIndex();
  }

  /**
   * Used by leader node to try to commit entries.
   *
   * @param leaderCommit leader's commitIndex
   * @param term the entry's term which index is leaderCommit in leader's log module
   * @return true or false
   */
  public synchronized boolean maybeCommit(long leaderCommit, long term) {
    if (leaderCommit > commitIndex && matchTerm(term, leaderCommit)) {
      try {
        commitTo(leaderCommit);
      } catch (LogExecutionException e) {
        // exceptions are ignored on follower side
      }
      return true;
    }
    return false;
  }

  /**
   * Overwrites the contents of this object with those of the given snapshot.
   *
   * @param snapshot leader's snapshot
   */
  public void applySnapshot(Snapshot snapshot) {
    logger.info(
        "{}: log module starts to restore snapshot [index: {}, term: {}]",
        name,
        snapshot.getLastLogIndex(),
        snapshot.getLastLogTerm());
    try {
      lock.writeLock().lock();

      long localIndex = commitIndex;
      long snapIndex = snapshot.getLastLogIndex();
      if (localIndex >= snapIndex) {
        logger.info("requested snapshot is older than the existing snapshot");
        return;
      }

      entries.subList(1, entries.size()).clear();
      entries.set(0, new EmptyEntry(snapshot.getLastLogIndex(), snapshot.getLastLogTerm()));

      this.commitIndex = snapshot.getLastLogIndex();

      // as the follower receives a snapshot, the logs persisted is not complete, so remove them
      if (config.isEnableRaftLogPersistence()) {
        getStableEntryManager().clearAllLogs(commitIndex);
      }

      synchronized (changeApplyCommitIndexCond) {
        this.appliedIndex = snapshot.getLastLogIndex();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Determines if the given (lastTerm, lastIndex) log is more up-to-date by comparing the index and
   * term of the last entries in the existing logs. If the logs have last entries with different
   * terms, then the log with the later term is more up-to-date. If the logs end with the same term,
   * then whichever log has the larger lastIndex is more up-to-date. If the logs are the same, the
   * given log is up-to-date.
   *
   * @param lastTerm candidate's lastTerm
   * @param lastIndex candidate's lastIndex
   * @return true or false
   */
  public boolean isLogUpToDate(long lastTerm, long lastIndex) {
    return lastTerm > getLastLogTerm()
        || (lastTerm == getLastLogTerm() && lastIndex >= getLastLogIndex());
  }

  /**
   * Pack entries from low through high - 1, just like slice (entries[low:high]). firstIndex <= low
   * <= high <= lastIndex.
   *
   * @param low request index low bound
   * @param high request index upper bound
   */
  public List<Entry> getEntries(long low, long high) {
    if (low >= high) {
      return Collections.emptyList();
    }
    try {
      lock.readLock().lock();
      long localFirst = getFirstIndex();
      long localLast = getLastLogIndex();
      low = Math.max(low, localFirst);
      high = Math.min(high, localLast + 1);
      return new ArrayList<>(entries.subList((int) (low - localFirst), (int) (high - localFirst)));
    } finally {
      lock.readLock().unlock();
    }
  }

  public Entry getEntryUnsafe(long index) {
    return entries.get((int) (index - getFirstIndex()));
  }

  private long entrySize(long low, long hi) {
    long entryMemory = 0;
    for (Entry entry : getEntries(low, hi)) {
      entryMemory += entry.estimateSize();
    }
    return entryMemory;
  }

  private int maxLogNumShouldReserve(long maxMemSize) {
    long totalSize = 0;
    for (int i = entries.size() - 1; i >= 1; i--) {
      if (totalSize + entries.get(i).estimateSize() > maxMemSize) {
        return entries.size() - 1 - i;
      }
      totalSize += entries.get(i).estimateSize();
    }
    return entries.size() - 1;
  }

  private void checkCompaction(List<Entry> entries) {
    boolean needToCompactLog = false;
    // calculate the number of old committed entries to be reserved by entry number
    int numToReserveForNew = minNumOfLogsInMem;
    if (entries.size() > maxNumOfLogsInMem) {
      needToCompactLog = true;
      numToReserveForNew = maxNumOfLogsInMem - entries.size();
    }

    // calculate the number of old committed entries to be reserved by entry size
    long newEntryMemSize = 0;
    for (Entry entry : entries) {
      newEntryMemSize += entry.estimateSize();
    }

    int sizeToReserveForNew = minNumOfLogsInMem;
    if (newEntryMemSize + committedEntrySize > maxLogMemSize) {
      needToCompactLog = true;
      sizeToReserveForNew = maxLogNumShouldReserve(maxLogMemSize - newEntryMemSize);
    }

    // reserve old committed entries with the minimum number
    if (needToCompactLog) {
      int numForNew = Math.min(numToReserveForNew, sizeToReserveForNew);
      int sizeToReserveForConfig = minNumOfLogsInMem;
      innerDeleteLog(Math.min(sizeToReserveForConfig, numForNew));
    }
  }

  private void removedCommitted(List<Entry> entries) {
    long commitLogIndex = getCommitLogIndex();
    long firstLogIndex = entries.get(0).getCurrLogIndex();
    if (commitLogIndex >= firstLogIndex) {
      logger.warn(
          "Committing logs that has already been committed: {} >= {}",
          commitLogIndex,
          firstLogIndex);
      entries
          .subList(0, (int) (getCommitLogIndex() - entries.get(0).getCurrLogIndex() + 1))
          .clear();
    }
  }

  private void commitEntries(List<Entry> entries) throws LogExecutionException {
    try {
      // Operations here are so simple that the execution could be thought
      // success or fail together approximately.
      Entry lastLog = entries.get(entries.size() - 1);
      commitIndex = lastLog.getCurrLogIndex();

      if (config.isEnableRaftLogPersistence()) {
        // Cluster could continue provide service when exception is thrown here
        getStableEntryManager().append(entries, appliedIndex);
      }
      for (Entry entry : entries) {
        if (entry.createTime != 0) {
          entry.committedTime = System.nanoTime();
          Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_COMMIT.add(
              entry.committedTime - entry.createTime);
        }
      }
    } catch (IOException e) {
      // The exception will block the raft service continue accept log.
      // TODO: Notify user that the persisted logs before these entries(include) are corrupted.
      // TODO: An idea is that we can degrade the service by disable raft log persistent for
      // TODO: the group. It needs fine-grained control for the config of Raft log persistence.
      logger.error("{}: persistent raft log error:", name, e);
      throw new LogExecutionException(e);
    }
  }

  /**
   * Used by MaybeCommit or MaybeAppend or follower to commit newly committed entries.
   *
   * @param newCommitIndex request commitIndex
   */
  public void commitTo(long newCommitIndex) throws LogExecutionException {
    if (commitIndex >= newCommitIndex) {
      return;
    }

    try {
      lock.writeLock().lock();
      long lo = commitIndex + 1;
      long hi = newCommitIndex + 1;
      List<Entry> entries = new ArrayList<>(getEntries(lo, hi));

      if (entries.isEmpty()) {
        return;
      }

      removedCommitted(entries);
      checkCompaction(entries);
      commitEntries(entries);
      applyEntries(entries);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns whether the index and term passed in match.
   *
   * @param term request entry term
   * @param index request entry index
   * @return true or false
   */
  public boolean matchTerm(long term, long index) {
    long t;
    try {
      t = getTerm(index);
    } catch (Exception e) {
      return false;
    }
    return t == term;
  }

  /**
   * Used by commitTo to apply newly committed entries
   *
   * @param entries applying entries
   */
  void applyEntries(List<Entry> entries) {
    for (Entry entry : entries) {
      applyEntry(entry);
    }

    long unappliedLogSize = getCommitLogIndex() - appliedIndex;
    if (unappliedLogSize > config.getMaxNumOfLogsInMem()) {
      logger.info(
          "There are too many unapplied logs [{}], wait for a while to avoid memory overflow",
          unappliedLogSize);
      try {
        synchronized (changeApplyCommitIndexCond) {
          changeApplyCommitIndexCond.wait(
              Math.min((unappliedLogSize - config.getMaxNumOfLogsInMem()) / 10 + 1, 1000));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void applyEntry(Entry entry) {
    // For add/remove logs in data groups, this log will be applied immediately when it is
    // appended to the raft log.
    // In this case, it will apply a log that has been applied.
    if (entry.isApplied()) {
      return;
    }
    try {
      logApplier.apply(entry);
    } catch (Exception e) {
      entry.setException(e);
      entry.setApplied(true);
    }
  }

  /**
   * findConflict finds the index of the conflict. It returns the first pair of conflicting entries
   * between the existing entries and the given entries, if there are any. If there is no
   * conflicting entries, and the existing entries contains all the given entries, -1 will be
   * returned. If there is no conflicting entries, but the given entries contains new entries, the
   * index of the first new entry will be returned. An entry is considered to be conflicting if it
   * has the same index but a different term. The index of the given entries MUST be continuously
   * increasing.
   *
   * @param entries request entries
   * @return -1 or conflictIndex
   */
  long findConflict(List<Entry> entries) {
    for (Entry entry : entries) {
      if (!matchTerm(entry.getCurrLogTerm(), entry.getCurrLogIndex())) {
        if (entry.getCurrLogIndex() <= getLastLogIndex()) {
          logger.info("found conflict at index {}", entry.getCurrLogIndex());
        }
        return entry.getCurrLogIndex();
      }
    }
    return -1;
  }

  public void close() {
    getStableEntryManager().close();
    if (deleteLogExecutorService != null) {
      deleteLogExecutorService.shutdownNow();
      if (deleteLogFuture != null) {
        deleteLogFuture.cancel(true);
      }

      try {
        deleteLogExecutorService.awaitTermination(20, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Close delete log thread interrupted");
      }
      deleteLogExecutorService = null;
    }

    if (checkLogApplierExecutorService != null) {
      checkLogApplierExecutorService.shutdownNow();
      checkLogApplierFuture.cancel(true);
      try {
        checkLogApplierExecutorService.awaitTermination(20, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Close check log applier thread interrupted");
      }
      checkLogApplierExecutorService = null;
    }

    if (logApplier != null) {
      logApplier.close();
    }
  }

  public StableEntryManager getStableEntryManager() {
    return stableEntryManager;
  }

  private void setStableEntryManager(StableEntryManager stableEntryManager) {
    this.stableEntryManager = stableEntryManager;
  }

  public long getAppliedIndex() {
    return appliedIndex;
  }

  public long getAppliedTerm() {
    return appliedTerm;
  }

  /** check whether delete the committed log */
  void checkDeleteLog() {
    try {
      lock.writeLock().lock();
      if (appliedIndex - getFirstIndex() <= minNumOfLogsInMem) {
        return;
      }
      innerDeleteLog(minNumOfLogsInMem);
    } catch (Exception e) {
      logger.error("{}, error occurred when checking delete log", name, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void innerDeleteLog(int sizeToReserve) {
    long appliedLogNum = appliedIndex - getFirstIndex();
    long removeSize = appliedLogNum - sizeToReserve;
    if (removeSize <= 0) {
      return;
    }

    long compactIndex = getFirstIndex() + removeSize;
    logger.debug(
        "{}: Before compaction index {}-{}, compactIndex {}, removeSize {}, committedLogSize "
            + "{}, maxAppliedLog {}",
        name,
        getFirstIndex(),
        getLastLogIndex(),
        compactIndex,
        removeSize,
        commitIndex - getFirstIndex(),
        appliedIndex);
    compactEntries(compactIndex);
    if (config.isEnableRaftLogPersistence()) {
      getStableEntryManager().removeCompactedEntries(compactIndex);
    }
    logger.debug(
        "{}: After compaction index {}-{}, committedLogSize {}",
        name,
        getFirstIndex(),
        getLastLogIndex(),
        commitIndex - getFirstIndex());
  }

  void compactEntries(long compactIndex) {
    long firstIndex = getFirstIndex();
    if (compactIndex < firstIndex) {
      logger.info(
          "entries before request index ({}) have been compacted, and the compactIndex is ({})",
          firstIndex,
          compactIndex);
      return;
    }
    long lastLogIndex = getLastLogIndex();
    if (compactIndex >= lastLogIndex) {
      logger.info("compact ({}) is out of bound lastIndex ({})", compactIndex, lastLogIndex);
      compactIndex = lastLogIndex - 1;
    }
    int index = (int) (compactIndex - firstIndex);
    for (int i = 0; i < index; i++) {
      committedEntrySize -= entries.get(0).estimateSize();
    }
    if (index > 0) {
      entries.subList(0, index).clear();
    }
  }

  public Object getLogUpdateCondition(long logIndex) {
    return logUpdateConditions[(int) (logIndex % logUpdateConditions.length)];
  }

  void applyAllCommittedLogWhenStartUp() {
    long lo = appliedIndex;
    long hi = commitIndex + 1;
    if (lo >= hi) {
      logger.info(
          "{}: the maxHaveAppliedCommitIndex={}, lastIndex={}, no need to reapply",
          name,
          appliedIndex,
          hi);
      return;
    }

    List<Entry> entries = new ArrayList<>(getEntries(lo, hi));
    applyEntries(entries);
  }

  public void checkAppliedLogIndex() {
    while (!Thread.interrupted()) {
      try {
        doCheckAppliedLogIndex();
      } catch (IndexOutOfBoundsException e) {
        // ignore
      } catch (Exception e) {
        logger.error("{}, an exception occurred when checking the applied log index", name, e);
      }
    }
    logger.info(
        "{}, the check-log-applier thread {} is interrupted",
        name,
        Thread.currentThread().getName());
  }

  void doCheckAppliedLogIndex() {
    long nextToCheckIndex = appliedIndex + 1;
    try {
      if (nextToCheckIndex > commitIndex) {
        // avoid spinning
        Thread.sleep(100);
        return;
      }
      Entry log = getEntryUnsafe(nextToCheckIndex);
      if (log == null || log.getCurrLogIndex() != nextToCheckIndex) {
        logger.debug(
            "{}, get log error when checking the applied log index, log={}, nextToCheckIndex={}",
            name,
            log,
            nextToCheckIndex);
        return;
      }
      if (!log.isApplied() && appliedIndex < log.getCurrLogIndex()) {
        synchronized (log) {
          while (!log.isApplied() && appliedIndex < log.getCurrLogIndex()) {
            // wait until the log is applied or a newer snapshot is installed
            log.wait(10);
          }
        }
      }
      if (nextToCheckIndex > appliedIndex) {
        synchronized (changeApplyCommitIndexCond) {
          // maxHaveAppliedCommitIndex may change if a snapshot is applied concurrently
          if (nextToCheckIndex > appliedIndex) {
            appliedTerm = log.getCurrLogTerm();
            appliedIndex = nextToCheckIndex;
          }
        }
      }

      logger.debug(
          "{}: log={} is applied, nextToCheckIndex={}, commitIndex={}, maxHaveAppliedCommitIndex={}",
          name,
          log,
          nextToCheckIndex,
          commitIndex,
          appliedIndex);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("{}: do check applied log index is interrupt", name);
    }
  }

  public String getName() {
    return name;
  }

  public ReentrantReadWriteLock getLock() {
    return lock;
  }
}