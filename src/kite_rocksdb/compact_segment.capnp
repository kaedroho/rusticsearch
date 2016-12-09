@0xa06431226eb55bb7;

struct CompactSegment {
    termDirectories @0 :List(TermDirectory);
    struct TermDirectory {
        field @0 :Int32;
        term @1 :Int32;
        directory @2: Data;
    }

    storedFieldValues @1 :List(StoredFieldValue);
    struct StoredFieldValue {
        doc @0 :UInt16;
        field @1 :Int32;
        valueType @2 :Text;
        value @3: Data;
    }

    statistics @2 :List(Statistic);
    struct Statistic {
        name @0 :Text;
        value @1 :Int64;
    }
}
