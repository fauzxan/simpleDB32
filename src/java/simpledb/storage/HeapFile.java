package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

//import javax.xml.catalog.Catalog;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tD;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tD = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tD;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        // some code goes here
        HeapPage page = null;
        IllegalArgumentException t = new IllegalArgumentException();

        int pgNo = pid.getPageNumber();
        long pos = pgNo * BufferPool.getPageSize();
        byte[] temp_file = new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "r");  //Open our file with read/write access
            long max = raf.length();
            if(pos >= max)
                throw t; 
            raf.seek(pos);
            raf.read(temp_file, 0, temp_file.length);   //Read from our RAF up to the length of our array
            raf.close();    //Close our filestream.
            
            page = new HeapPage((HeapPageId) pid, temp_file);

         } catch (Exception e) {       
            e.printStackTrace();
         } 
        // Adding the table to the catalog here
//        Database.getCatalog().addTable(this, file.getName());

        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        long pos =  pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            raf.seek(pos);
            raf.write(data);
            raf.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            return (int) file.length()/BufferPool.getPageSize();
        }
        catch(Exception e){
            return 0;
        }
    }

    // see DbFile.java for javadocs
    //find a page with an empty slot or if no such pages exist in the HeapFile
    //you need to create a new page and append it to the physical file on disk
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = null;
        HeapPageId pid;

        int pg=0;
        while(pg<numPages()){
            pid = new HeapPageId(getId(), pg);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            //check for empty page
            //TO CHECK >> catch DbException instead?
            if(page.getNumEmptySlots()>0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                page.markDirty(true, tid);
                break;
            } else{
                //if a transaction t finds no free slot on a page p, t may immediately release the lock on p
                Database.getBufferPool().unsafeReleasePage(tid, pid);
            }
            pg += 1;
        }

        //all pages were full, create new page
        if(pg==numPages()) {
            pid = new HeapPageId(getId(), pg);
            //TO CHECK >> do this here or in readpage?
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
            //add new to buffer pool
//            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            page.markDirty(true, tid);
            writePage(page);
        }

        //TO CHECK >> change interface to return single page?
        ArrayList<Page> modpages = new ArrayList<>();
        modpages.add(page);
        return modpages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if (page==null)
            throw new DbException("no page");
        page.deleteTuple(t);
        //TO CHECK >> change interface to return single page?
        ArrayList<Page> modpages = new ArrayList<>();
        modpages.add(page);
        return modpages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        /** The whole implementation over here was changed from using nested iterators to 
         * using page numbers to call the pages from the bufferpool instead. This allows 
         * us to follow the Page limit given for in bufferpool allowing appropriate preventing
         * running out of memory error. The old implementation has been commented out and the 
         * new implementation is below it.
        */
        //TO CHECK >> a bit messy way, could make use of page numbers instead?
        DbFileIterator dbfileiter = new DbFileIterator(){

            // Iterator<Iterator<Tuple>> page_iter;
            Iterator<Tuple> tup_iter;
            int pageNo = 0;
            boolean isOpen = false;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                // ArrayList<Iterator<Tuple>> page_arr = new ArrayList<Iterator<Tuple>>();
                // for(int i = 0; i<numPages(); ++i){
                //     HeapPageId pp =  new HeapPageId(getId(), i);
                //     HeapPage temp_page = (HeapPage) Database.getBufferPool().getPage(tid, pp, Permissions.READ_ONLY);
                //     page_arr.add(temp_page.iterator());
                // }
                // page_iter = page_arr.iterator();
                // tup_iter = page_iter.next();
                // page_arr = null;

                tup_iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), 0), Permissions.READ_ONLY)).iterator();
                isOpen = true;

            }

            @Override
            public void close() {
                // Not sure what to write here
                // page_iter = null;
                tup_iter = null;
                pageNo = -1;
                isOpen = false;

            }
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // if(page_iter == null || tup_iter == null)
                //     return false;
                // while(page_iter.hasNext()==true || tup_iter.hasNext()==true){
                //     try{
                //         if(tup_iter.hasNext()){
                //             return true;
                //         }
                //         else{
                //             if(page_iter.hasNext()) {
                //                 tup_iter = page_iter.next();
                //             }
                //         }
                //         if(tup_iter == null)
                //             return false;
                //     }
                //     catch(Exception e){
                //         return false;
                //     }
                // }
                // return false;

                if (tup_iter == null) {
                    return false;
                }

                while (!tup_iter.hasNext()) {
                    pageNo+=1;
                    if (pageNo >= numPages()) {
                        return false;
                    }
                    tup_iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY)).iterator();
                }
                return true;

            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // try {
                //     if(tup_iter.hasNext()){
                //         return tup_iter.next();
                //     }
                //     else{
                //         if(page_iter.hasNext()){
                //             tup_iter = page_iter.next();
                //             return tup_iter.next();
                //         }
                //     }
                // }
                // catch(Exception e){
                //     throw new NoSuchElementException();
                // }
                // throw new NoSuchElementException();
                try{
                    if (tup_iter.hasNext()){
                        return tup_iter.next();
                    }
                }
                catch(Exception e){
                    throw new NoSuchElementException();
                }
                throw new NoSuchElementException();

            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }
        };

        return dbfileiter;
    }
}

