package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
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
    private File file;
    private TupleDesc tupleDesc;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        //Lab-1 Exercise 5
        this.file = f;
        this.numPages = (int)(file.length()/BufferPool.getPageSize());
        this.tupleDesc =td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        //Lab-1 Exercise 5
        return this.file;
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
        //Lab-1 Exercise 5
        return this.file.getAbsoluteFile().hashCode();
//    throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        //Lab-1 Exercise 5
        return this.tupleDesc;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        //Lab-1 Exercise 5
        HeapPage page = null;

        int pgNo = pid.getPageNumber();
        long offset = pgNo * BufferPool.getPageSize(); // Arbitary Byte Offset
        byte[] byte_file = new byte[BufferPool.getPageSize()]; // Creating a new Byte File which is an array of bytes as RAF works with the file as a large array of bytes stored in the file system.
        try {
            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "r");  //Creates a RandomAccessFile with read/write access
            long file_size = raf.length();
            if (offset>file_size)
            {
                throw new IllegalArgumentException("The PageID is invalid|HeapFile.Java|readPage(PageId)");
            }
            raf.seek(offset);
            raf.read(byte_file, 0, byte_file.length);
            page = new HeapPage((HeapPageId) pid,byte_file);

         } catch (Exception e)
         {
            e.printStackTrace();
         }
         return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        //Lab-1 Exercise 5
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    /**
 * HeapFileIterator is an implementation of a DbFileIterator that allows you to
 * iterate through the tuples of each page in the HeapFile.
 */
    public class HeapFileIterator implements DbFileIterator

    {
        Iterator<Tuple> tuple_iter;
        private TransactionId tid;
        private HeapFile file;
        private Iterator<Tuple> tup_iter;
        Boolean isOpen = false;
        private int pgNo;

    /**
     * Constructs a heap file iterator
     *
     * @param tid
     *            the trasnsaction id
     * @param file
     *            the heap file to iterate over
     */
        public HeapFileIterator(TransactionId tid, HeapFile file)
        {
            this.tid = tid;
            this.file = file;
            this.pgNo = 0; // Intialize page number to 0, to iterate from the beginning of the file.

        }

        @Override
        public void open() throws DbException, TransactionAbortedException
        {
            HeapPageId pageId= new HeapPageId(this.file.getId(),this.pgNo); // Generates the HeapPageId of the first page in the file
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid,pageId, Permissions.READ_ONLY);// Initialises the first page in the HeapPage
            this.tup_iter = page.iterator(); //Initialises the tuple iterator of the first page
            this.isOpen = true;
        }
        @Override
        public void close() {
            this.tup_iter = null;
            this.isOpen = false;
            this.pgNo = 0;
        }
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.isOpen == false){
                return false;
            }
            if (tup_iter == null)
            {
                return false; //If there is no tuple iterator return false
            }
            if (!this.tup_iter.hasNext()) //If tuple iterator.hasNext for a page is false, i.e you have reached the end of the page
            {
                this.pgNo += 1; // Increment the page number by 1
                if (this.pgNo >= this.file.numPages())
                {
                    return false; //If the current page number is greater than the total number of pages in the file return false
                }
                HeapPageId pageId= new HeapPageId(this.file.getId(),this.pgNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid,pageId, Permissions.READ_ONLY); //Initialise the heap page for the new page
                this.tup_iter = page.iterator(); //Initialise the iterator for the new page
            }
            return true; // Returns true if number of pages <= total number of pages in the file and if there are more tuples in the given page.
        }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                try{
                    if (this.hasNext()){
                        return this.tup_iter.next(); //return the next tuple given that it exists
                    }
                }
                catch(Exception e){
                    throw new NoSuchElementException();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close(); //Resets all the class attributes
                this.open();  //Re-initialises the class attributes
            }

        }
     // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        //Lab-1 Exercise 5
        HeapFileIterator file_iter = new HeapFileIterator(tid,this);
        return file_iter; // Returns a new heap file iterator
    }

}

