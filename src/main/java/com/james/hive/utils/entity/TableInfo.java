package com.james.hive.utils.entity;

import com.google.gson.annotations.Expose;

import java.util.List;

public class TableInfo{
    private String dbName;
    private String tableName;
    private String tableType;
    private String format;
    private String location;
    private String inputFormat;
    private String outputFormat;
    private List<ColsInfo> colsInfo;
    private boolean compressed;
    private String priPartKey;
    private SerdeParams serdeParams;
    private String serdeLib;
    private String priPartType;
    private String subPartKey;
    private String subPartType;
    private PartsInfo partsInfo;
    private FieldDelimiter fieldDelimiter;
    private String createTime;
    private String tableOwner;
    private String tableComment;

    public String getDbName(){
        return dbName;
    }
    public void setDbName(String input){
        this.dbName = input;
    }
    public String getTableName(){
        return tableName;
    }
    public void setTableName(String input){
        this.tableName = input;
    }
    public String getTableType(){
        return tableType;
    }
    public void setTableType(String input){
        this.tableType = input;
    }
    public String getFormat(){
        return format;
    }
    public void setFormat(String input){
        this.format = input;
    }
    public String getLocation(){
        return location;
    }
    public void setLocation(String input){
        this.location = input;
    }
    public String getInputFormat(){
        return inputFormat;
    }
    public void setInputFormat(String input){
        this.inputFormat = input;
    }
    public String getOutputFormat(){
        return outputFormat;
    }
    public void setOutputFormat(String input){
        this.outputFormat = input;
    }
    public List<ColsInfo> getColsInfo(){
        return colsInfo;
    }
    public void setColsInfo(List<ColsInfo> input){
        this.colsInfo = input;
    }
    public boolean getCompressed(){
        return compressed;
    }
    public void setCompressed(boolean input){
        this.compressed = input;
    }
    public String getPriPartKey(){
        return priPartKey;
    }
    public void setPriPartKey(String input){
        this.priPartKey = input;
    }
    public SerdeParams getSerdeParams(){
        return serdeParams;
    }
    public void setSerdeParams(SerdeParams input){
        this.serdeParams = input;
    }
    public String getSerdeLib(){
        return serdeLib;
    }
    public void setSerdeLib(String input){
        this.serdeLib = input;
    }
    public String getPriPartType(){
        return priPartType;
    }
    public void setPriPartType(String input){
        this.priPartType = input;
    }
    public String getSubPartKey(){
        return subPartKey;
    }
    public void setSubPartKey(String input){
        this.subPartKey = input;
    }
    public String getSubPartType(){
        return subPartType;
    }
    public void setSubPartType(String input){
        this.subPartType = input;
    }
    public PartsInfo getPartsInfo(){
        return partsInfo;
    }
    public void setPartsInfo(PartsInfo input){
        this.partsInfo = input;
    }
    public FieldDelimiter getFieldDelimiter(){
        return fieldDelimiter;
    }
    public void setFieldDelimiter(FieldDelimiter input){
        this.fieldDelimiter = input;
    }
    public String getCreateTime(){
        return createTime;
    }
    public void setCreateTime(String input){
        this.createTime = input;
    }
    public String getTableOwner(){
        return tableOwner;
    }
    public void setTableOwner(String input){
        this.tableOwner = input;
    }
    public String getTableComment(){
        return tableComment;
    }
    public void setTableComment(String input){
        this.tableComment = input;
    }
}