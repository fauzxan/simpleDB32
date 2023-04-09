//package simpledb.storage;
//
//import simpledb.common.Database;
//import simpledb.common.DbException;
//import simpledb.common.Permissions;
//import simpledb.transaction.TransactionAbortedException;
//import simpledb.transaction.TransactionId;
//
//import java.io.*;
//import java.util.*;
//
//
//
///**
// * HeapFile is an implementation of a DbFile that stores a collection of tuples
// * in no particular order. Tuples are stored on pages, each of which is a fixed
// * size, and the file is simply a collection of those pages. HeapFile works
// * closely with HeapPage. The format of HeapPages is described in the HeapPage
// * constructor.
// *
// * @see HeapPage#HeapPage
// * @author Sam Madden
// */
//public class HeapFile implements DbFile {
//    private File file;
//    private TupleDesc tupleDesc;
//    ArrayList<Page> changedPages;
//
//    /**
//     * Constructs a heap file backed by the specified file.
//     *
//     * @param f
//     *            the file that stores the on-disk backing store for this heap
//     *            file.
//     */
//    public HeapFile(File f, TupleDesc td) {
//        //Lab-1 Exercise 5
//        this.file = f;
//        this.tupleDesc = td;
//        this.changedPages = new ArrayList<Page>();
//    }
//
//    /**
//     * Returns the File backing this HeapFile on disk.
//     *
//     * @return the File backing this HeapFile on disk.
//     */
//    public File getFile() {
//        //Lab-1 Exercise 5
//        return this.file;
//    }
//
//    /**
//     * Returns an ID uniquely identifying this HeapFile. Implementation note:
//     * you will need to generate this tableid somewhere to ensure that each
//     * HeapFile has a "unique id," and that you always return the same value for
//     * a particular HeapFile. We suggest hashing the absolute file name of the
//     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
//     *
//     * @return an ID uniquely identifying this HeapFile.
//     */
//    public int getId() {
//        //Lab-1 Exercise 5
//        return this.file.getAbsoluteFile().hashCode();
//    }
//
//    /**
//     * Returns the TupleDesc of the table stored in this DbFile.
//     *
//     * @return TupleDesc of this DbFile.
//     */
//    public TupleDesc getTupleDesc() {
//        //Lab-1 Exercise 5
//        return this.tupleDesc;
//    }
//
//    public Page readPage(PageId pid) throws IllegalArgumentException {
//        //Lab-1 Exercise 5
//        HeapPage page = null;
//
//        int pgNo = pid.getPageNumber();
//        long offset = pgNo * BufferPool.getPageSize(); // Arbitary Byte Offset
//        byte[] byte_file = new byte[BufferPool.getPageSize()]; // Creating a new Byte File which is an array of bytes as RAF works with the file as a large array of bytes stored in the file system.
//        try {
//            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "r");  //Creates a RandomAccessFile with read/write access
//            long file_size = raf.length();
//            if (offset>file_size)
//            {
//                throw new IllegalArgumentException("The PageID is invalid | HeapFile.Java | readPage(PageId)");
//            }
//            raf.seek(offset);
//            raf.read(byte_file, 0, byte_file.length);
//            page = new HeapPage((HeapPageId) pid,byte_file);
//
//         } catch (Exception e)
//         {
//            e.printStackTrace();
//         }
//         return page;
//    }
//
//    // see DbFile.java for javadocs
//    public void writePage(Page page) throws IOException {
//        PageId pid = page.getId();
//        long pos =  pid.getPageNumber() * BufferPool.getPageSize();
//        byte[] data = page.getPageData();
//        try {
//            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
//            raf.seek(pos);
//            raf.write(data);
//            raf.close();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Returns the number of pages in this HeapFile.
//     */
//    public int numPages() {
//        //Lab-1 Exercise 5
//        try {
//            return (int) file.length()/BufferPool.getPageSize();
//        }
//        catch(Exception e){
//            return 0;
//        }
//    }
//
//    // see DbFile.java for javadocs
//    // Modification of this function: markDirty is already being called by BuffePool. So, commenting out here for now to
//    // see if it breaks the code first.
//    public List<Page> insertTuple(TransactionId tid, Tuple t)
//            throws DbException, IOException, TransactionAbortedException {
//        //Lab-2 Exercise 3
//        HeapPage page = null;
//        HeapPageId pid;
//
//        int pg=0;
//        while(pg<this.numPages()){
//            pid = new HeapPageId(getId(), pg);
//            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//            if(page.getNumEmptySlots()>0) {
//                page.insertTuple(t);
////                page.markDirty(true, tid);
//                break;
//            }
//            pg += 1;
//        }
//
//        //All pages were full, create new page
//        if(pg==this.numPages()) {
//            pid = new HeapPageId(getId(), pg);
//            page = new HeapPage(pid, HeapPage.createEmptyPageData());
//            page.insertTuple(t);
////            page.markDirty(true, tid);
//            writePage(page);
//        }
//        this.changedPages.add(page);
//        return changedPages;
//
//    }
//
//    // see DbFile.java for javadocs
//    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
//            TransactionAbortedException {
//        PageId pid = t.getRecordId().getPageId();
//        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//        if (page==null)
//            throw new DbException("Not a valid tuple| HeapFile.Java | deleteTuple(TransactionId tid, Tuple t)");
//        page.deleteTuple(t);
////        ArrayList<Page> pages = new ArrayList<>();
//        this.changedPages.add(page);
//        return changedPages;
//    }
//
//    /**
// * HeapFileIterator is an implementation of a DbFileIterator that allows you to
// * iterate through the tuples of each page in the HeapFile.
// */
//    public class HeapFileIterator implements DbFileIterator
//
//    {
//        Iterator<Tuple> tuple_iter;
//        private TransactionId tid;
//        private HeapFile file;
//        private Iterator<Tuple> tup_iter;
//        Boolean isOpen = false;
//        private int pgNo;
//
//    /**
//     * Constructs a heap file iterator
//     *
//     * @param tid
//     *            the trasnsaction id
//     * @param file
//     *            the heap file to iterate over
//     */
//        public HeapFileIterator(TransactionId tid, HeapFile file)
//        {
//            this.tid = tid;
//            this.file = file;
//            this.pgNo = 0; // Intialize page number to 0, to iterate from the beginning of the file.
//
//        }
//
//        @Override
//        public void open() throws DbException, TransactionAbortedException
//        {
//            HeapPageId pageId= new HeapPageId(this.file.getId(),this.pgNo); // Generates the HeapPageId of the first page in the file
//            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid,pageId, Permissions.READ_ONLY);// Initialises the first page in the HeapPage
//            this.tup_iter = page.iterator(); //Initialises the tuple iterator of the first page
//            this.isOpen = true;
//        }
//        @Override
//        public void close() {
//            this.tup_iter = null;
//            this.isOpen = false;
//            this.pgNo = 0;
//        }
//        @Override
//        public boolean hasNext() throws DbException, TransactionAbortedException {
//            if (this.isOpen == false){
//                return false;
//            }
//            if (tup_iter == null)
//            {
//                return false; //If there is no tuple iterator return false
//            }
//            if (!this.tup_iter.hasNext()) //If tuple iterator.hasNext for a page is false, i.e you have reached the end of the page
//            {
//                this.pgNo += 1; // Increment the page number by 1
//                if (this.pgNo >= this.file.numPages())
//                {
//                    return false; //If the current page number is greater than the total number of pages in the file return false
//                }
//                HeapPageId pageId= new HeapPageId(this.file.getId(),this.pgNo);
//                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid,pageId, Permissions.READ_ONLY); //Initialise the heap page for the new page
//                this.tup_iter = page.iterator(); //Initialise the iterator for the new page
//            }
//            return true; // Returns true if number of pages <= total number of pages in the file and if there are more tuples in the given page.
//        }
//
//            @Override
//            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
////                try{
////                    if (this.hasNext()){
//                        return this.tup_iter.next(); //return the next tuple given that it exists
////                    }
////                }
////                catch(Exception e){
////                    throw new NoSuchElementException("No more tuples in HeapFile | HeapFile.java | next()");
////                }
////                throw new NoSuchElementException();
//           }
//
//            @Override
//            public void rewind() throws DbException, TransactionAbortedException {
//                this.close(); //Resets all the class attributes
//                this.open();  //Re-initialises the class attributes
//            }
//
//        }
//     // see DbFile.java for javadocs
//    /**
//     * Returns an iterator over all the tuples stored in this DbFile. The
//     * iterator must use {@link BufferPool#getPage}, rather than
//     * {@link #readPage} to iterate through the pages.
//     *
//     * @return an iterator over all the tuples stored in this DbFile.
//     */
//    public DbFileIterator iterator(TransactionId tid) {
//        //Lab-1 Exercise 5
//        HeapFileIterator file_iter = new HeapFileIterator(tid,this);
//        return file_iter; // Returns a new heap file iterator
//    }
//
//}
//







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
                Database.getBufferPool().unsafe_unpin(pid);
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

