package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */


public class Catalog {

    // Marked for review- did not find implementation of getId() anywhere else


    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public ConcurrentHashMap<Integer, Table> catalog;
    public class Table{
        TupleDesc td;
        String primaryKey;
        String name;
        DbFile dbFile;
        public Table(TupleDesc td, String pk, String name, DbFile dbFile){
            this.td = td;
            this.primaryKey = pk;
            this.name = name;
            this.dbFile = dbFile;
        }
    }


    /**
     * Carry out the necessary checks for creating the table
     * @param file file cannot be null
     * @param name must not be null. It can be empty
     * @param pkeyField Cannot be null or empty
     * @return An instantiated Table object
     */
    public Table createTable(DbFile file, String name, String pkeyField) throws NullPointerException{
        if (file == null || name == null || pkeyField == null){
            throw new NullPointerException("Null value during the creation of Table object!");
        }
        if (pkeyField == ""){
//            throw new NullPointerException("Empty primary key field detected!");
            System.out.println("Warning: Primary Key field is '' | Catalog.java | createTable(file, name, pkeyField)");
        }
        for (Integer key: this.catalog.keySet()){
            if (this.catalog.get(key).name == name){
                this.catalog.remove(key);
            }

        }
        return new Table(file.getTupleDesc(), pkeyField, name, file);
    }




    public Catalog() {
        // some code goes here
        this.catalog = new ConcurrentHashMap<Integer, Table>();
    }



    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * If there exists a table with the same name or ID, replace that old table with this one. 
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile. 
     * @param name the name of the table -- may be an empty string.  May not be null.  
     * @param pkeyField the name of the primary key field
     */


    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (this.catalog.containsKey(file.getId())){
            this.catalog.remove(file.getId());
        }
        this.catalog.put(Integer.valueOf(file.getId()), this.createTable(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here

        for (Integer key: this.catalog.keySet()){
            System.out.println("Name is:"+this.catalog.get(key).name);
            if (this.catalog.get(key).name == name) {
                return key;
            }
        }

        throw new NoSuchElementException("While looking for name within catalog, no such name was found!");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     * This method returns the entire value present at that file ID
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!this.catalog.containsKey(tableid)){
            throw new NoSuchElementException("Couldn't find key with parameter passed | Catalog.java | getTupleDesc(tableid)");
        }
        else{
            return this.catalog.get(tableid).td;
        }

    }
    /**
     * Expected: simpledb.storage.TupleDesc<Fields: null(INT_TYPE), null(INT_TYPE), 2 Fields in total>
     * Result:   simpledb.storage.TupleDesc<Fields: null(INT_TYPE), null(INT_TYPE), 2 Fields in total>
     */


    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!this.catalog.containsKey(tableid)){
            throw new NoSuchElementException("While looking carrying out getDatabaseFile, couldn't find key with parameter passed");
        }
        else{
            return this.catalog.get(tableid).dbFile;
        }
    }

    public String getPrimaryKey(int tableid) throws NoSuchElementException{
        // some code goes here
        if (!this.catalog.containsKey(tableid)){
            throw new NoSuchElementException("While looking carrying out getPrimaryKey, couldn't find key with parameter passed");
        }
        else{
            return this.catalog.get(tableid).primaryKey;
        }
    }

    public Iterator<Integer> tableIdIterator(){
        // some code goes here
            return this.catalog.keySet().iterator();

    }

    public String getTableName(int id) throws NoSuchElementException{
        // some code goes here
        if (this.catalog.containsKey(id)){
            return this.catalog.get(id).name;
        }
        else{
            throw new NoSuchElementException("While looking carrying out getTableName, couldn't find key with parameter passed");
        }
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        for (Integer key: this.catalog.keySet()){
            this.catalog.remove(key);
        }
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

