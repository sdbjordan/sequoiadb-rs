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

impl BsonType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Eoo),
            1 => Some(Self::Double),
            2 => Some(Self::String),
            3 => Some(Self::Object),
            4 => Some(Self::Array),
            5 => Some(Self::Binary),
            6 => Some(Self::Undefined),
            7 => Some(Self::ObjectId),
            8 => Some(Self::Boolean),
            9 => Some(Self::Date),
            10 => Some(Self::Null),
            11 => Some(Self::Regex),
            12 => Some(Self::Ref),
            13 => Some(Self::Code),
            14 => Some(Self::Symbol),
            15 => Some(Self::CodeWithScope),
            16 => Some(Self::Int32),
            17 => Some(Self::Timestamp),
            18 => Some(Self::Int64),
            100 => Some(Self::Decimal),
            127 => Some(Self::MaxKey),
            255 => Some(Self::MinKey),
            _ => None,
        }
    }
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
