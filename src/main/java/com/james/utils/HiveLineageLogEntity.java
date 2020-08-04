package com.james.utils;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.Date;

/**
 * Created by James on 20-7-25 下午10:15
 * <p>
 * `id` int(11) NOT NULL AUTO_INCREMENT,
 * `lineage_str` longtext NOT NULL,
 * `lineage_str_sha` longtext NOT NULL,
 * `update_time` datetime NOT NULL COMMENT '更新时间',
 */
public class HiveLineageLogEntity implements Comparable<HiveLineageLogEntity> {
    private int id;
    private String lineageStr;
    private String lineageStrSha;
    private Date updateTime;

    public HiveLineageLogEntity() {
    }

    public HiveLineageLogEntity(String lineageStr) {
        this.lineageStr = lineageStr;
        this.lineageStrSha = DigestUtils.sha1Hex(lineageStr.getBytes());
        this.updateTime = new Date();
    }

    public HiveLineageLogEntity(String lineageStr, String lineageStrSha, Date updateTime) {
        this.lineageStr = lineageStr;
        this.lineageStrSha = lineageStrSha;
        this.updateTime = updateTime;
    }

    @Override
    public int compareTo(HiveLineageLogEntity o) {
        return this.getId() - o.getId();
    }

    @Override
    public String toString() {
        return String.format("id=%s, lineageStr=%s, lineageStrSha=%s, updateTime=%s", id, lineageStr, lineageStrSha, updateTime);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLineageStr() {
        return lineageStr;
    }

    public void setLineageStr(String lineageStr) {
        this.lineageStr = lineageStr;
    }

    public String geLineageStrSha() {
        return lineageStrSha;
    }

    public void setGetLineageStrSha(String lineageStrSha) {
        this.lineageStrSha = lineageStrSha;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
