package com.nr8.analytics.r8port.config.impl;

public class ConsulKVRecord {

  private int CreateIndex;
  private int ModifyIndex;
  private int LockIndex;
  private String Key;
  private int Flags;
  private String Value;

  public int getCreateIndex() {
    return CreateIndex;
  }

  public void setCreateIndex(int createIndex) {
    CreateIndex = createIndex;
  }

  public int getModifyIndex() {
    return ModifyIndex;
  }

  public void setModifyIndex(int modifyIndex) {
    ModifyIndex = modifyIndex;
  }

  public int getLockIndex() {
    return LockIndex;
  }

  public void setLockIndex(int lockIndex) {
    LockIndex = lockIndex;
  }

  public String getKey() {
    return Key;
  }

  public void setKey(String key) {
    Key = key;
  }

  public int getFlags() {
    return Flags;
  }

  public void setFlags(int flags) {
    Flags = flags;
  }

  public String getValue() {
    return Value;
  }

  public void setValue(String value) {
    Value = value;
  }
}
