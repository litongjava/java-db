package com.litongjava.db.activerecord.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DbBaseEntity {
  private String remark;

  private String creator;
  private String updater;
  private java.sql.Timestamp createTime;
  private java.sql.Timestamp updateTime;

  private Short deleted;

  private Long tenantId;

  public String baseInfo() {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("(")
        //
        .append("remark=" + remark).append(",creator=" + creator).append(",updater=" + updater)
        //
        .append(",createTime=" + createTime).append(",updateTime=" + updateTime)
        //
        .append(",deleted=" + deleted).append(",tenantId=" + tenantId)
        //
        .append(")");

    return stringBuffer.toString();
  }
}
