package simpledb.common;

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
import java.util.concurrent.ConcurrentHashMap;

/*
Allowed situations:
 1. TrA is reading
 2. TrA, TrB are reading
 3. TrA is reading and writing
 */
public class LockManager {

    /** Mapping of each page to its locks **/
    private HashMap<PageId, PageLocks> pageLocksMap;
    /** Mapping of transaction to all pages it holds locks on **/
    private HashMap<TransactionId, HashSet<PageId>> transactionsMap;
    private HashMap<PageId, HashSet<TransactionId>> waitlist;

    private static final long TIMEOUT_MILLIS = 100;

    class PageLocks{
        private TransactionId writeLock;
        private HashSet<TransactionId> readLocks;

        PageLocks(){
            writeLock = null;
            readLocks = new HashSet<>();
        }

        void setWriteLock(TransactionId tid){
            writeLock = tid;
        }

        void setReadLock(TransactionId tid){
            readLocks.add(tid);
        }

        HashSet<TransactionId> getReadLock(TransactionId tid){
            return (HashSet<TransactionId>) readLocks.clone();
        }

        void releaseWriteLock(TransactionId tid){
            if(holdsWriteLock(tid)) {
                writeLock = null;
            }
        }

        void releaseReadLock(TransactionId tid){
            readLocks.remove(tid);
        }

        boolean holdsWriteLock(TransactionId tid){
            if (writeLock==null)
                return false;
            return writeLock.equals(tid);
        }

        boolean holdsReadLock(TransactionId tid){
            return readLocks.contains(tid);
        }
    }

    public LockManager(){
        pageLocksMap = new HashMap<>();
        transactionsMap = new HashMap<>();
        waitlist = new HashMap<>();
    }

    /** Returns true if transaction has any kind of lock (read/write/both) on page **/
    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        PageLocks currLocks = pageLocksMap.get(pid);
        if (currLocks==null)
            return false;
        return currLocks.holdsReadLock(tid) || currLocks.holdsWriteLock(tid);
    }

    public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm) throws InterruptedException, TransactionAbortedException {
        pageLocksMap.putIfAbsent(pid, new PageLocks());
        transactionsMap.putIfAbsent(tid, new HashSet<>());
        waitlist.putIfAbsent(pid, new HashSet<TransactionId>());

        HashSet<PageId> currPages = transactionsMap.get(tid);
        PageLocks currLocks = pageLocksMap.get(pid);
        HashSet<TransactionId> currWait = waitlist.get(pid);

        // if trA writing, trB cannot read

        if(perm==Permissions.READ_ONLY){
            if(((currLocks.writeLock!=null) || (currWait.size()!=0)) && !(currLocks.holdsWriteLock(tid) || currLocks.holdsReadLock(tid))){
                while(currLocks.writeLock!=null || (currWait.size()!=0)){
                    if(deadlock(pid,tid)){
                        notify();
                        throw new TransactionAbortedException();
                    }
                    wait(TIMEOUT_MILLIS);
                }
            }
            currLocks.setReadLock(tid);
            currPages.add(pid);
            return;
        }
        else if(perm==Permissions.READ_WRITE){

            if(currLocks.holdsWriteLock(tid)){
                currWait.remove(tid);
                currLocks.setReadLock(tid);
                currPages.add(pid);
                return;
            }
            currWait.add(tid);
            if((currLocks.writeLock!=null)  && (!currLocks.holdsWriteLock(tid))){
                while(currLocks.writeLock!=null){
                    if(deadlock(pid,tid)){
                        notify();
                        throw new TransactionAbortedException();
                    }
                    wait(TIMEOUT_MILLIS);
                }
            }
            currLocks.setWriteLock(tid);
            currLocks.readLocks.remove(tid);
            HashSet<TransactionId> p_lock = currLocks.getReadLock(tid); // Returns a clone, therefore removing tid does not affect locks
            p_lock.remove(tid);
            while(!p_lock.isEmpty()){
                if(deadlock(pid,tid)){

                    currWait.remove(tid);
                    currLocks.releaseWriteLock(tid);
                    notify();
                    throw new TransactionAbortedException();
                }
                wait(TIMEOUT_MILLIS);
                p_lock = currLocks.getReadLock(tid);
                p_lock.remove(tid);
            }
            currWait.remove(pid);
            currLocks.setReadLock(tid);
            currPages.add(pid);
            return;
        }
    }

    public synchronized boolean upgrade(TransactionId tid, PageId pid){
        PageLocks currLocks = pageLocksMap.get(pid);

        if (currLocks==null || currLocks.holdsWriteLock(tid) || !currLocks.holdsReadLock(tid))
            return false;

        // transaction has read lock and no one else has a read lock
        if (currLocks.readLocks.size()==1) {
            currLocks.setWriteLock(tid);
            return true;
        }
        return false;
    }

    /** To release ALL LOCKS held on SPECIFIED PAGE by SPECIFIED TRANSACTION **/
    public synchronized void release(TransactionId tid, PageId pid){
        //if transaction holds read and write locks, releases both
        PageLocks currLocks = pageLocksMap.get(pid);
        currLocks.releaseReadLock(tid);
        currLocks.releaseWriteLock(tid);

        HashSet<PageId> currPages = transactionsMap.get(tid);
        HashSet<TransactionId> currWait = waitlist.get(pid);
        currWait.remove(tid);
        currPages.remove(pid);
        notifyAll();
    }

    /** To release ALL LOCKS held on ALL PAGES by SPECIFIED TRANSACTION **/
    public synchronized void releaseAll(TransactionId tid) {
        HashSet<PageId> currPages = transactionsMap.get(tid);
        if(currPages == null){
            return;
        }

        Iterator<PageId> page_iter = currPages.iterator();
        while (page_iter.hasNext()) {
            // try disconnecting vertices
            PageId pid = page_iter.next();
            PageLocks currLocks = pageLocksMap.get(pid);
            currLocks.releaseReadLock(tid);
            currLocks.releaseWriteLock(tid);
            HashSet<TransactionId> currWait = waitlist.get(pid);
            currWait.remove(tid);
        }
        transactionsMap.remove(tid);
        transactionsMap.put(tid, new HashSet<>());
        notifyAll();
    }

    public synchronized boolean deadlock(PageId pid, TransactionId tid){
        HashSet<PageId> page_iter =  transactionsMap.get(tid);
        if(page_iter == null){ // if transaction has no resources, no deadlock
            return false;
        }

        for(PageId p: page_iter){

            for(TransactionId t :  waitlist.get(p)) {
                if (t == null) { // if no one wants this resource, move to next
                    continue;
                }
                if (t == tid){
                    continue;
                }
                if (transactionsMap.get(t).contains(pid)) { // if someone wants a resource and has our page, cycle exists
                    return true;
                }
                if (deadlock(pid, t))  // recursive call
                    return true;
            }
        }
        return false;
    }
}