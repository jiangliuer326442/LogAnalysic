package com.mustafa.bigdata;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

public class GreetBean implements Serializable {
    private Timestamp time;
    private String fuid;
    private String tuid;

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public String getFuid() {
        return fuid;
    }

    public void setFuid(String fuid) {
        this.fuid = fuid;
    }

    public String getTuid() {
        return tuid;
    }

    public void setTuid(String tuid) {
        this.tuid = tuid;
    }

    @Override
    public String toString() {
        return "GreetBean{" +
                "time=" + time +
                ", fuid='" + fuid + '\'' +
                ", tuid='" + tuid + '\'' +
                '}';
    }
}
