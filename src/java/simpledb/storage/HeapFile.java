package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.lang.System.Logger;
import java.util.*;
import java.util.random.RandomGenerator;
import java.util.logging.Level;


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
        // some code goes here
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
        // some code goes here
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
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
//    throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
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
        // some code goes here
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
    public class HeapFileIterator implements DbFileIterator

    {
        Iterator<Tuple> tuple_iter;
        private TransactionId tid;
        private HeapFile file;
        private Iterator<Tuple> tup_iter;
        Boolean isOpen;
        private int pgNo;


        public HeapFileIterator(TransactionId tid, HeapFile file)
        {
            this.tid = tid;
            this.file = file;
            this.pgNo = 0;

        }

        @Override
        public void open() throws DbException, TransactionAbortedException
        {
            HeapPageId pageId= new HeapPageId(this.file.getId(),this.pgNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid,pageId, Permissions.READ_ONLY);
            this.tup_iter = page.iterator();
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
                return false;
            }
            if (!this.tup_iter.hasNext())
            {
                this.pgNo += 1;
                if (this.pgNo >= this.file.numPages())
                {
                    return false;
                }
                HeapPageId pageId= new HeapPageId(this.file.getId(),this.pgNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid,pageId, Permissions.READ_ONLY);
                this.tup_iter = page.iterator();
            }
            return true;
        }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                try{
                    if (this.hasNext()){
                        return this.tup_iter.next();
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
        HeapFileIterator file_iter = new HeapFileIterator(tid,this);
        return file_iter;
    }

}

