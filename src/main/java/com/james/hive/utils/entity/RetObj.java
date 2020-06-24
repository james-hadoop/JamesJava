package com.james.hive.utils.entity;

public class RetObj{
    private String code;
    private String message;
    private Solution solution;
    private TableInfo tableInfo;

    public String getCode(){
        return code;
    }
    public void setCode(String input){
        this.code = input;
    }
    public String getMessage(){
        return message;
    }
    public void setMessage(String input){
        this.message = input;
    }
    public Solution getSolution(){
        return solution;
    }
    public void setSolution(Solution input){
        this.solution = input;
    }
    public TableInfo getTableInfo(){
        return tableInfo;
    }
    public void setTableInfo(TableInfo input){
        this.tableInfo = input;
    }
}