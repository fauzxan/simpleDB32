package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionId;


/**
 * I am going to work with the assumption that the lock manager has the ability to do the following:
 * 1. Acquire locks on pages. method: acquireLock(pageid)
 * 2. Release locks on pages. method: releaseLock(pageid)
 * 3. Maintain a list of pages that are locked- i.e., being written to. If a page is being read from,
 *      it doesn't need a lock, but rather it needs a version of it that is readable before the lock is acquired.
 *      So read-only mode makes it necessary for us to maintain a secondary list that has versions of pages before
 *      they got 'locked'.
 *      - ConcurrentHashMap<TransactionID, PageID> lockList : refers to list of pages that are locked.
 *                                                          lockList maintains all the tables that the transaction
 *                                                          is referring to.
 *      - ConcurrentHashMap<PageID, Page> previousImage : refers to the list of pages that can be accessed by those
 *                                                        transactions trying to access it from read-only mode.
 *
 */

/**
 * class LockManager{
 *     static lockTable: {
 *         PageId: LTEntry<numberoftransactions, natureOfLock, queueOfRequests-transactionIDs, Optional: hashSet of transactions currently holding lock on this page>
 *     }
 *     transactionTable: { // list of locks the transaction holds
 *          tid: List<PageID> // only grant lock to a transaction if the page is not already there in its list. Also
 *                               check for if
 *                               1. Request is for read only mode- then can issue a shared lock
 *                               2. Request is for write mode- then can issue an exclusive lock iff page in lockTable
 *                                  has numberOftransactions == 0, else add to transactionID queue.
 *     }
 *     Possible events (Should be included as methods.):
 *     1. Acquire - Update locktable and transaction table appropriately
 *     2. Release - Same
 *     3. Commit, Abort - transaction should release all the locks;
 *
 *     Note: If a t1 holds shared lock, and t2 requests exclusive lock, it must enter the queue in lockTable. If t3
 *          then requests for a shared lock, then it must again enter the queue, even though it is compatible with t1.
 *          This is to prevent t2 from starvation (waiting indefinitely for lock).
 *
 *     Important functionality for exercise 5:
 *     - Implement a waits for graph in lock manager. Nodes are transactions. Edge from node1 to node2 exists if node1
 *     is waiting for node2 to release a lock. Lock manager adds edges to the graph as requests are queued. And removes
 *     the edges when locks are granted.
 */

public class LockManager{
    static class LTEntry{
        int numberOfTransactions;
        boolean shared; // Indicates nature of lock
        ArrayList<TransactionId> requests;

    }
    static LockTable ConcurrentHashMap<PageID, >
}