package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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

    LockManager lockManager;

    /** Mapping of all pages being used **/
    private LinkedHashMap<PageId, Page> pageMap;

    /** Number of pages as passed to constructor **/
    private int maxPages;

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096; 

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** To iterate through pages for evicting pages */
    Iterator<PageId> pageIter = null;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pageMap = new LinkedHashMap<>(numPages);
        this.maxPages = numPages;
        this.lockManager = new LockManager();
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        try{
            lockManager.acquire(tid, pid, perm);
        } catch(InterruptedException e){
            e.printStackTrace();
        }

        //lookup
        Page page =  pageMap.get(pid);
        //add if new and space available
        if (page==null){
            if(pageMap.size()>=maxPages) {
                evictPage();
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = file.readPage(pid);
            pageMap.put(pid, page);
        }
        return page;
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
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //TO CHECK >> a lock means any or something specific? - yours does check whether the page is locked so its fine
        return lockManager.holdsLock(tid, pid);
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

        //supporting code still required here
        try{
            if (commit){
                flushPages(tid);
            }
            else{
                Iterator<PageId> page_iter = pageMap.keySet().iterator();
                while (page_iter.hasNext()) {
                    PageId pid = page_iter.next();
                    Page p = pageMap.get(pid);
                    if (p.isDirty() != null && p.isDirty().equals(tid)) {
                        Page temp = p.getBeforeImage();
                        pageMap.remove(pid);
                        pageMap.put(pid, temp);
                        // discardPage(pid);
                        // getPage(tid, pid, Permissions.READ_ONLY);
                    }
                }
            }
        }
        catch(Exception e){
            ;
        }
        lockManager.releaseAll(tid);
        // CHECK -> Should there be a set image for all the pages after committing them. If yes, should be here

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
        // not necessary for lab1
        List<Page> modpages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page: modpages) {
            page.markDirty(true, tid);

            if (pageMap.size() > maxPages) {
                evictPage();
            }
            
            pageMap.put(page.getId(), page);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
       try {
           int tableid = t.getRecordId().getPageId().getTableId();
           List<Page> modpages = Database.getCatalog().getDatabaseFile(tableid).deleteTuple(tid, t);
           for (Page page : modpages) {
               //mark dirty and update in cache(bufferpool map)
               page.markDirty(true, tid);
               pageMap.put(page.getId(), page);
           }
       } catch(NullPointerException e){
           throw new DbException("tuple not in any table");
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
        for(PageId pid: pageMap.keySet()){
            flushPage(pid);
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
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        if (!pageMap.containsKey(pid)){
            return;
        }

        Page p = pageMap.get(pid);
//        if (p.isDirty() == null){
//            p.setBeforeImage();
//            p.markDirty(false, null);
//            return;
//        }
//
//        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
//        file.writePage(p);
//        p.setBeforeImage();
//        p.markDirty(false, null);
        if(p.isDirty()!=null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        Iterator<PageId> page_iter = pageMap.keySet().iterator();
        while (page_iter.hasNext()) {
            
            PageId pid = page_iter.next();
            Page p = pageMap.get(pid);
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        // pageIter = pageMap.keySet().iterator();

        // while (pageIter.hasNext()) {
        //     PageId pid = pageIter.next();
        //     if (pageMap.get(pid).isDirty() == null) {
        //         pageMap.remove(pid);
        //         return;
        //     }
        //     try {
        //         flushPage(pid);
        //     } catch (IOException e) {
        //         throw new DbException("Error during eviction");
        //     }
        //     pageMap.remove(pid);
        //     return;
        // }
        // throw new DbException("All pages are dirty");

        // Instead of just taking the first element in the list, 
        // we will iterate through the HashMap and find non dirty page
        // Order of going through the pages is from first page added to the last page added
        Iterator<PageId> page_iter = pageMap.keySet().iterator(); 
        while(page_iter.hasNext()){
            PageId pid = page_iter.next();

            // Dirty pages are not removed, hence are skipped
            if (pageMap.get(pid).isDirty() != null) {
                continue;
            }
            try {
                flushPage(pid);
            } catch (IOException e) {
                throw new DbException("Error during eviction");
            }
            discardPage(pid);
            return;
        }

        throw new DbException("No non-dirty pages in the buffer pool");
        
    }

}
