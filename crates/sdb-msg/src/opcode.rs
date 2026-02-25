/// Message operation codes matching SequoiaDB protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum OpCode {
    // Requests
    Insert = 2001,
    Query = 2004,
    Delete = 2006,
    Update = 2002,
    GetMore = 2005,
    KillContext = 2007,
    Disconnect = 2008,
    Aggregate = 2019,
    TransBegin = 2010,
    TransCommit = 2011,
    TransRollback = 2012,
    CreateCS = 2013,
    DropCS = 2014,
    CreateCL = 2015,
    DropCL = 2016,
    CreateIndex = 2017,
    DropIndex = 2018,
    // Reply
    Reply = 2500,
    // Internal
    CatalogReq = 3001,
    CatalogReply = 3002,
    ReplSync = 4001,
    ReplConsistency = 4002,
}
