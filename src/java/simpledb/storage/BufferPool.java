package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.io.IOException;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    /**
     * Frame contains the following:
     * @param timeStamp that indicates the time at which the page was last put in the buffer pool. Keep in mind that the
     *                  timeStamp refers to the last used time. Thus, the timestamp must be set only when unpinning.
     */
    private class Frame{
        private long timeStamp;
        private int pincount;
        private Page page;
        public Frame(Page page){
            this.page = page;
            // this.timeStamp = System.currentTimeMillis(); // TimeStamp must only be initialized upon unpinning.
            this.pincount = 1;
        }
        public long getTimeStamp(){
            return this.timeStamp;
        }
        public void pinFrame(){
            this.pincount ++;
        }

        public void unpinFrame(){
            if (this.pincount != 0){
                this.pincount --;
            }
            this.timeStamp = System.currentTimeMillis();
        }

        public int getPincount() {
            return this.pincount;
        }

        public Page getPage(){
            return this.page;
        }
    }


    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private ConcurrentHashMap<PageId, Page> cache;
    private ConcurrentHashMap<PageId, Frame> LRUCache;
    private int numPages;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        //Lab-1 Exercise 3
        this.numPages = numPages; // Max Pages that can be cached
        this.cache = new ConcurrentHashMap<PageId, Page>(); // Creates a new Cache
        this.LRUCache = new ConcurrentHashMap<PageId, Frame>(); // Creates a new LRU cache, need to replace the cache above
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm) throws DbException{
        //Lab-1 Exercise 3
        if (this.LRUCache.containsKey(pid)) {
            return this.LRUCache.get(pid).getPage();
        }
        else {
            // Writes the page onto cache and returns it
            if (this.LRUCache.size() == this.numPages){
                // Page eviction policy needs to be implemented

                this.evictPage(); // if the eviction is not possible due to NO STEAL, then a DbException will be thrown
                // at evictPage() and no new pages will be added to the buffer pool

            }
            if (this.LRUCache.size() < this.numPages){
                DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page newPage = dbfile.readPage(pid);
                Frame newFrame = new Frame(newPage);
                this.LRUCache.put(pid, newFrame);
                return newPage;
            }
            return null;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modpages = file.insertTuple(tid, t);

        for (Page page: modpages) {
            if (page.isDirty() == null) {
                page.markDirty(true, tid);
            }

            if (this.LRUCache.size() == this.numPages) {
                this.evictPage();
            }
            else{
                Frame frame = new Frame(page);
                this.LRUCache.put(page.getId(), frame);
            }
        }


    }



    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        int tableId=t.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modpages = table.deleteTuple(tid, t);
        for (Page page : modpages) {
            if (page.isDirty() == null) {
                page.markDirty(true, tid);
            }
            if (this.LRUCache.size() == this.numPages) {
                this.evictPage();
            }
            Frame frame = new Frame(page);
            this.LRUCache.put(page.getId(), frame);
        }

    }
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pageId : this.LRUCache.keySet()){
            flushPage(pageId);
        }


    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        // this is not implemented with eviction policy because this is required for recovery and B+ tree only
        this.LRUCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.LRUCache.get(pid).getPage();
        int tableId = ((HeapPageId)pid).getTableId();
        HeapFile hpf = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
        hpf.writePage(page);
        page.markDirty(false, null);
        this.LRUCache.remove(pid);
    }

    /** Write all pages of the specified transaction to disk
     * @Fauzaan This should be part of FORCE policy; flushPages prolly be called from elsewhere.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @Fauzaan Tries to see if
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId minTSPageId = null; // keep track of the page with the minimum timestamp
        double minTS = Double.POSITIVE_INFINITY; // keep track of the minimum timestamp
        for (PageId pageId: this.LRUCache.keySet()){
            Frame f = this.LRUCache.get(pageId);
            if (f.getPage().isDirty() == null &&
                    (double) f.getTimeStamp() < minTS &&
                    f.getPincount() == 0
                    // must not be dirty, must have TS < minTS, must have pincount == 0
            ){
                minTSPageId = pageId;
                minTS = (double) f.getTimeStamp();
            }
        }
        if (minTS != Double.POSITIVE_INFINITY && minTSPageId != null){
            try{
                this.flushPage(minTSPageId);
            }catch(IOException e){
                System.out.println("IOException thrown when trying to flushPage() | BufferPool.java | evictPage()");
            }
        }
        else{
            throw new DbException("No pages to evict! | BufferPool.java | evictPage()");
        }
    }

}