package com.sequoiadb.driver.protocol;

public final class OpCode {
    public static final int MSG_REQ = 1000;
    public static final int UPDATE_REQ = 2001;
    public static final int INSERT_REQ = 2002;
    public static final int SQL_REQ = 2003;
    public static final int QUERY_REQ = 2004;
    public static final int GET_MORE_REQ = 2005;
    public static final int DELETE_REQ = 2006;
    public static final int KILL_CONTEXT_REQ = 2007;
    public static final int DISCONNECT = 2008;
    public static final int TRANS_BEGIN_REQ = 2010;
    public static final int TRANS_COMMIT_REQ = 2011;
    public static final int TRANS_ROLLBACK_REQ = 2012;
    public static final int AGGREGATE_REQ = 2019;

    public static final int REPLY_MASK = 0x40000000;

    private OpCode() {}
}
