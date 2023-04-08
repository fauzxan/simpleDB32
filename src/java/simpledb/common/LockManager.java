package simpledb.common;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.storage.BufferPool;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

public class LockManager {


    // The Key represents the resource (pageId), and LockState stores the transaction ID and lock type, so each LockState represents a lock added by a transaction on the page.
    // Therefore, the entire map represents the lock information of all the pages..
    private ConcurrentHashMap<PageId, List<LockState>> lockStateMap;
    private DependencyGraph dependencyGraph;

    // Key is the transactionID and the value is a PageId represents the resource being waited for, this is used for saving the waiting information. Note: in the BufferPool, waiting is actually implemented using sleep.
    private ConcurrentHashMap<TransactionId, PageId> waiting_map;

    public LockManager() {
        lockStateMap = new ConcurrentHashMap<>();
        waiting_map = new ConcurrentHashMap<>();
        this.dependencyGraph = new DependencyGraph();
    }

    // Returns true if tid already has a read lock on pid.
    // If tid already has a write lock on pid, or no lock but it can acquire a read lock,
    // then acquire the lock and return true. Return false if tid cannot acquire a read lock on pid.

    public boolean periodicCall(){
        return this.dependencyGraph.containsCycles(this.lockStateMap, this.waiting_map);
    }
    public synchronized boolean grant_shared_lock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {// If there is only one lock on pid
                LockState ls = list.iterator().next();
                if (ls.getTid().equals(tid)) {// Check if the lock on the given page id belongs to the given transaction id
                    //If it is a read lock, return directly the lock or otherwise acquire the lock and then return.
                    return ls.getPerm() == Permissions.READ_ONLY || lock(pid, tid, Permissions.READ_ONLY);
                } else {
                    //If it is someone else's read lock, acquire the lock and return true, otherwise it is a write lock and needs to wait.
                    return ls.getPerm() == Permissions.READ_ONLY ? lock(pid, tid, Permissions.READ_ONLY) : wait(tid, pid);
                }
            } else {
                /**
                //There are four scenarios:

                // There are two locks, and both belong to the same transaction (one read lock and one write lock).
                // There are two locks, and both belong to different transactions (one read lock and one write lock).
                // There are multiple read locks, and one of them belongs to the current transaction.
                // There are multiple read locks, and none of them belong to the current transaction.
                 */
                for (LockState ls : list) {
                    if (ls.getPerm() == Permissions.READ_WRITE) {
                        //if one of the locks is a write lock, then it depends on whether it belongs to the current transaction or not to determine whether it falls into situation 1 or situation 2.
                        return ls.getTid().equals(tid) || wait(tid, pid);
                    } else if (ls.getTid().equals(tid)) {//If it is a read lock and belongs to tid.
                        return true;//Return case 3 here, or it may be case 1 (if a read lock is traversed first)
                    }
                }
                // There are multiple read locks and none of them belong to the current transaction (tid).
                return lock(pid, tid, Permissions.READ_ONLY); // Acquire read lock
            }
        } else {
            return lock(pid, tid, Permissions.READ_ONLY);
        }
    }

    // If tid already has a write lock on pid, return true
    // If tid only has a read lock on pid or tid does not have a lock on pid but the condition allows tid to add a write lock to pid, add the lock and return true
    // If tid cannot add a write lock to pid at this time, return false
    public synchronized boolean grant_exclusive_lock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {//If there is only one lock on pid.
                LockState ls = list.iterator().next();
                // If there is only one lock on the pid
                // If it is my own write lock, return true directly
                // Otherwise, acquire the lock and return (return at the lock)
                // If this lock belongs to others, it must wait, which means returning at the wait statement (after the colon)
                return ls.getTid().equals(tid) ? ls.getPerm() == Permissions.READ_WRITE || lock(pid, tid, Permissions.READ_WRITE) : wait(tid, pid);
            } else {
                // There are three scenarios when there are multiple locks, only the first one returns true, the rest return wait:
                // 1. Two locks, both belong to tid (one read and one write)
                // 2. Two locks, both belong to a non-tid transaction (one read and one write)
                // 3. Multiple read locks
                if (list.size() == 2) {
                    for (LockState ls : list) {
                        if (ls.getTid().equals(tid) && ls.getPerm() == Permissions.READ_WRITE) {
                            return true;// If there are only two locks and one of them is a write lock owned by the transaction, return true, otherwise wait.
                        }
                    }
                }
                return wait(tid, pid);
            }
        } else {//If there is no lock on pid, add a write lock to pid for tid and return true.
            return lock(pid, tid, Permissions.READ_WRITE);
        }
    }


    /**
     * Locks the resource and returns true, indicating that tid has a lock on pid with permission perm.
     */
    private synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm) {
        LockState nls = new LockState(tid, perm);
        List<LockState> list = lockStateMap.get(pid);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(nls);
        lockStateMap.put(pid, list);
        waiting_map.remove(tid);
        System.out.println("Lock has been granted to a transaction successfully! | LockManager.java | lock(pid, tid, perm)");
        System.out.println(pid);
//        System.out.println(perm);
        return true;
    }

    //Just process the waiting_map and return false
    private synchronized boolean wait(TransactionId tid, PageId pid) {
        TransactionId source = tid;
//        List<LockState> lockStateList = this.lockStateMap.get(pid);
//        if (lockStateList.size() == 1){
//            this.dependencyGraph.insert(source, lockStateList.get(1));
//        }
        waiting_map.put(tid, pid);
        System.out.println("A transaction has been put into waitList | LockManager.java | wait(tid, pid)");

        return false;
    }


    //unlock is designed to be called at any time, and returns false if it does not exist.
    // This way, the code for finding whether it exists is already in the method, and there is no need to confirm its existence before unlocking elsewhere.
    // Instead, you should unlock first and then determine if it exists based on the return
    public synchronized boolean unlock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        for (LockState ls: list){
            System.out.println("INSIDE UNLOCK");
            System.out.println(ls.getPerm());
        }
        if (list == null || list.size() == 0) return false;
        LockState ls = getLockState(tid, pid);
        if (ls == null) return false;
        list.remove(ls);
        lockStateMap.put(pid, list);

        return true;
    }

    //Release all locks held by transaction tid.
    public synchronized List<PageId> releaseTransactionLocks(TransactionId tid) {
        //This method first finds all the locks that belong to tid and then releases them
        List<PageId> toRelease = getAllLocksByTid(tid);
        for (PageId pid : toRelease) {
            System.out.println("The line that never gets called");
            unlock(tid, pid);

        }
        return toRelease;
//        this.dependencyGraph.remove(tid);
    }

    //Returns the lock held by the transaction represented by tid on the given page pid.
    // If the lock does not exist, return null.
    public synchronized LockState getLockState(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list == null || list.size() == 0) {
            return null;
        }
        for (LockState ls : list) {
            if (ls.getTid().equals(tid)) {//Corresponding lock is found, return it
                return ls;
            }
        }
        return null;
    }

    // Returns all the pages on which tid has locks.
    private synchronized List<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        for (Map.Entry<PageId, List<LockState>> entry : lockStateMap.entrySet()) {
            for (LockState ls : entry.getValue()) {
                if (ls.getTid().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }

}