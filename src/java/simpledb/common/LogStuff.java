package simpledb.common;

import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.io.IOException;

class LogStuff{
    private Logger logger;
    private FileHandler fileHandler;
    public LogStuff(String name){
        this.logger = Logger.getLogger(this.getClass().getName());
        String fileName = "./logs/" + name;
        try{
            this.fileHandler = new FileHandler(fileName);
        }catch(IOException e){
            System.out.println("Logger failed to initialize");
        }
    }
}