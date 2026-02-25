/// BSON element types matching SequoiaDB's BSON variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BsonType {
    Eoo = 0,
    Double = 1,
    String = 2,
    Object = 3,
    Array = 4,
    Binary = 5,
    Undefined = 6,
    ObjectId = 7,
    Boolean = 8,
    Date = 9,
    Null = 10,
    Regex = 11,
    Ref = 12,
    Code = 13,
    Symbol = 14,
    CodeWithScope = 15,
    Int32 = 16,
    Timestamp = 17,
    Int64 = 18,
    Decimal = 100,
    MinKey = 255,
    MaxKey = 127,
}

/// 12-byte BSON ObjectId.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId {
    pub bytes: [u8; 12],
}

impl ObjectId {
    pub fn new() -> Self {
        // Stub: returns zero ObjectId
        Self { bytes: [0u8; 12] }
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}
