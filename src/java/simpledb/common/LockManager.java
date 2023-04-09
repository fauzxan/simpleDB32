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
    private HashMap<PageId, PageLocks> pageLock;
    /** Mapping of transaction to all pages it holds locks on **/
    private HashMap<TransactionId, HashSet<PageId>> transactions;
    private HashMap<PageId, HashSet<TransactionId>> waitList;

    private static final long TIMEOUT_MILLIS = 100;

    class PageLocks{
        private TransactionId exclusiveLock;
        private HashSet<TransactionId> sharedLock;

        PageLocks(){
            exclusiveLock = null;
            sharedLock = new HashSet<>();
        }

        void setExclusiveLock(TransactionId tid){
            exclusiveLock = tid;
        }

        void setSharedLock(TransactionId tid){
            sharedLock.add(tid);
        }

        HashSet<TransactionId> getSharedLock(TransactionId tid){
            return (HashSet<TransactionId>) sharedLock.clone();
        }

        void releaseExclusiveLock(TransactionId tid){
            if(holdsExclusiveLock(tid)) {
                exclusiveLock = null;
            }
        }

        void releaseReadLock(TransactionId tid){
            sharedLock.remove(tid);
        }

        boolean holdsExclusiveLock(TransactionId tid){
            if (exclusiveLock ==null)
                return false;
            return exclusiveLock.equals(tid);
        }

        boolean holdsSharedLock(TransactionId tid){
            return sharedLock.contains(tid);
        }
    }

    public LockManager(){
        pageLock = new HashMap<>();
        transactions = new HashMap<>();
        waitList = new HashMap<>();
    }

    /** Returns true if transaction has any kind of lock (read/write/both) on page **/
    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        PageLocks currLocks = pageLock.get(pid);
        if (currLocks==null)
            return false;
        return currLocks.holdsSharedLock(tid) || currLocks.holdsExclusiveLock(tid);
    }

    public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm) throws InterruptedException, TransactionAbortedException {
        pageLock.putIfAbsent(pid, new PageLocks());
        transactions.putIfAbsent(tid, new HashSet<>());
        waitList.putIfAbsent(pid, new HashSet<TransactionId>());

        HashSet<PageId> currPages = transactions.get(tid);
        PageLocks currLocks = pageLock.get(pid);
        HashSet<TransactionId> currWait = waitList.get(pid);


        if(perm==Permissions.READ_ONLY){
            if(((currLocks.exclusiveLock !=null) || (currWait.size()!=0)) && !(currLocks.holdsExclusiveLock(tid) || currLocks.holdsSharedLock(tid))){
                while(currLocks.exclusiveLock !=null || (currWait.size()!=0)){
                    if(deadlock(pid,tid)){
                        notify();
                        throw new TransactionAbortedException("TA");
                    }
                    wait(TIMEOUT_MILLIS);
                }
            }
            currLocks.setSharedLock(tid);
            currPages.add(pid);
            return;
        }
        else if(perm==Permissions.READ_WRITE){

            if(currLocks.holdsExclusiveLock(tid)){
                currWait.remove(tid);
                currLocks.setSharedLock(tid);
                currPages.add(pid);
                return;
            }
            currWait.add(tid);
            if((currLocks.exclusiveLock !=null)  && (!currLocks.holdsExclusiveLock(tid))){
                while(currLocks.exclusiveLock !=null){
                    if(deadlock(pid,tid)){
                        notify();
                        throw new TransactionAbortedException("TA");
                    }
                    wait(TIMEOUT_MILLIS);
                }
            }
            currLocks.setExclusiveLock(tid);
            currLocks.sharedLock.remove(tid);
            HashSet<TransactionId> p_lock = currLocks.getSharedLock(tid); // Returns a clone, therefore removing tid does not affect locks
            p_lock.remove(tid);
            while(!p_lock.isEmpty()){
                if(deadlock(pid,tid)){

                    currWait.remove(tid);
                    currLocks.releaseExclusiveLock(tid);
                    notify();
                    throw new TransactionAbortedException("TA");
                }
                wait(TIMEOUT_MILLIS);
                p_lock = currLocks.getSharedLock(tid);
                p_lock.remove(tid);
            }
            currWait.remove(pid);
            currLocks.setSharedLock(tid);
            currPages.add(pid);
            return;
        }
    }

    public synchronized boolean upgrade(TransactionId tid, PageId pid){
        PageLocks currLocks = pageLock.get(pid);

        if (currLocks==null || currLocks.holdsExclusiveLock(tid) || !currLocks.holdsSharedLock(tid))
            return false;

        // transaction has read lock and no one else has a read lock
        if (currLocks.sharedLock.size()==1) {
            currLocks.setExclusiveLock(tid);
            return true;
        }
        return false;
    }

    /** To release ALL LOCKS held on SPECIFIED PAGE by SPECIFIED TRANSACTION **/
    public synchronized void release(TransactionId tid, PageId pid){
        //if transaction holds read and write locks, releases both
        PageLocks currLocks = pageLock.get(pid);
        currLocks.releaseReadLock(tid);
        currLocks.releaseExclusiveLock(tid);

        HashSet<PageId> currPages = transactions.get(tid);
        HashSet<TransactionId> currWait = waitList.get(pid);
        currWait.remove(tid);
        currPages.remove(pid);
        notifyAll();
    }

    /** To release ALL LOCKS held on ALL PAGES by SPECIFIED TRANSACTION **/
    public synchronized void releaseAll(TransactionId tid) {
        HashSet<PageId> currPages = transactions.get(tid);
        if(currPages == null){
            return;
        }

        Iterator<PageId> page_iter = currPages.iterator();
        while (page_iter.hasNext()) {
            // try disconnecting vertices
            PageId pid = page_iter.next();
            PageLocks currLocks = pageLock.get(pid);
            currLocks.releaseReadLock(tid);
            currLocks.releaseExclusiveLock(tid);
            HashSet<TransactionId> currWait = waitList.get(pid);
            currWait.remove(tid);
        }
        transactions.remove(tid);
        transactions.put(tid, new HashSet<>());
        notifyAll();
    }

    public synchronized boolean deadlock(PageId pid, TransactionId tid){
        HashSet<PageId> page_iter =  transactions.get(tid);
        if(page_iter == null){ // if transaction has no resources, no deadlock
            return false;
        }

        for(PageId p: page_iter){

            for(TransactionId t :  waitList.get(p)) {
                if (t == null) { // if no one wants this resource, move to next
                    continue;
                }
                if (t == tid){
                    continue;
                }
                if (transactions.get(t).contains(pid)) { // if someone wants a resource and has our page, cycle exists
                    return true;
                }
                if (deadlock(pid, t))  // recursive call
                    return true;
            }
        }
        return false;
    }
}