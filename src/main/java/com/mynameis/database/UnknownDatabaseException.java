package com.mynameis.database;

public class UnknownDatabaseException extends Exception{
    public UnknownDatabaseException(){
        super();
    }

    public UnknownDatabaseException(String msg){
        super(msg);
    }
}
