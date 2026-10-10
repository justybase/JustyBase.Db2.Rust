/// IBM Db2 SQL type identifiers from the Go reference driver.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SqlType(pub u16);

impl SqlType {
    pub const fn is_nullable(self) -> bool {
        self.0 & 1 != 0
    }
    pub const fn base_type(self) -> Self {
        if self.is_nullable() {
            Self(self.0 - 1)
        } else {
            self
        }
    }
    pub fn name(self) -> String {
        match self.0 {
            384 => "DATE",
            385 => "DATE (NULLABLE)",
            388 => "TIME",
            389 => "TIME (NULLABLE)",
            392 => "TIMESTAMP",
            393 => "TIMESTAMP (NULLABLE)",
            396 => "DATALINK",
            397 => "DATALINK (NULLABLE)",
            404 => "BLOB",
            405 => "BLOB (NULLABLE)",
            408 => "CLOB",
            409 => "CLOB (NULLABLE)",
            412 => "DBCLOB",
            413 => "DBCLOB (NULLABLE)",
            448 => "VARCHAR",
            449 => "VARCHAR (NULLABLE)",
            452 => "CHAR",
            453 => "CHAR (NULLABLE)",
            456 => "LONG VARCHAR",
            457 => "LONG VARCHAR (NULLABLE)",
            460 => "CSTR",
            461 => "CSTR (NULLABLE)",
            464 => "VARGRAPHIC",
            465 => "VARGRAPHIC (NULLABLE)",
            468 => "GRAPHIC",
            469 => "GRAPHIC (NULLABLE)",
            472 => "LONG GRAPHIC",
            473 => "LONG GRAPHIC (NULLABLE)",
            476 => "LSTR",
            477 => "LSTR (NULLABLE)",
            480 => "FLOAT",
            481 => "FLOAT (NULLABLE)",
            484 => "DECIMAL",
            485 => "DECIMAL (NULLABLE)",
            488 => "ZONED",
            489 => "ZONED (NULLABLE)",
            492 => "BIGINT",
            493 => "BIGINT (NULLABLE)",
            496 => "INTEGER",
            497 => "INTEGER (NULLABLE)",
            500 => "SMALLINT",
            501 => "SMALLINT (NULLABLE)",
            504 => "NUMERIC",
            505 => "NUMERIC (NULLABLE)",
            904 => "ROWID",
            905 => "ROWID (NULLABLE)",
            908 => "VARBINARY",
            909 => "VARBINARY (NULLABLE)",
            912 => "BINARY",
            913 => "BINARY (NULLABLE)",
            960 => "BLOB LOCATOR",
            961 => "BLOB LOCATOR (NULLABLE)",
            964 => "CLOB LOCATOR",
            965 => "CLOB LOCATOR (NULLABLE)",
            968 => "DBCLOB LOCATOR",
            969 => "DBCLOB LOCATOR (NULLABLE)",
            988 => "XML",
            989 => "XML (NULLABLE)",
            996 => "DECFLOAT",
            997 => "DECFLOAT (NULLABLE)",
            2000 => "FakeUDT",
            2001 => "FakeNUDT",
            2436 => "BOOLEAN",
            2437 => "BOOLEAN (NULLABLE)",
            value => return format!("SQLType({value})"),
        }
        .to_string()
    }
}
pub const DATE: SqlType = SqlType(384);
pub const NDATE: SqlType = SqlType(385);
pub const TIME: SqlType = SqlType(388);
pub const NTIME: SqlType = SqlType(389);
pub const TIMESTAMP: SqlType = SqlType(392);
pub const NTIMESTAMP: SqlType = SqlType(393);
pub const DATALINK: SqlType = SqlType(396);
pub const NDATALINK: SqlType = SqlType(397);
pub const BLOB: SqlType = SqlType(404);
pub const NBLOB: SqlType = SqlType(405);
pub const CLOB: SqlType = SqlType(408);
pub const NCLOB: SqlType = SqlType(409);
pub const DBCLOB: SqlType = SqlType(412);
pub const NDBCLOB: SqlType = SqlType(413);
pub const VARCHAR: SqlType = SqlType(448);
pub const NVARCHAR: SqlType = SqlType(449);
pub const CHAR: SqlType = SqlType(452);
pub const NCHAR: SqlType = SqlType(453);
pub const LONG: SqlType = SqlType(456);
pub const NLONG: SqlType = SqlType(457);
pub const CSTR: SqlType = SqlType(460);
pub const NCSTR: SqlType = SqlType(461);
pub const VARGRAPH: SqlType = SqlType(464);
pub const NVARGRAPH: SqlType = SqlType(465);
pub const GRAPHIC: SqlType = SqlType(468);
pub const NGRAPHIC: SqlType = SqlType(469);
pub const LONGRAPH: SqlType = SqlType(472);
pub const NLONGRAPH: SqlType = SqlType(473);
pub const LSTR: SqlType = SqlType(476);
pub const NLSTR: SqlType = SqlType(477);
pub const FLOAT: SqlType = SqlType(480);
pub const NFLOAT: SqlType = SqlType(481);
pub const DECIMAL: SqlType = SqlType(484);
pub const NDECIMAL: SqlType = SqlType(485);
pub const ZONED: SqlType = SqlType(488);
pub const NZONED: SqlType = SqlType(489);
pub const BIGINT: SqlType = SqlType(492);
pub const NBIGINT: SqlType = SqlType(493);
pub const INTEGER: SqlType = SqlType(496);
pub const NINTEGER: SqlType = SqlType(497);
pub const SMALL: SqlType = SqlType(500);
pub const NSMALL: SqlType = SqlType(501);
pub const NUMERIC: SqlType = SqlType(504);
pub const NNUMERIC: SqlType = SqlType(505);
pub const ROWID: SqlType = SqlType(904);
pub const NROWID: SqlType = SqlType(905);
pub const VARBINARY: SqlType = SqlType(908);
pub const NVARBINARY: SqlType = SqlType(909);
pub const BINARY: SqlType = SqlType(912);
pub const NBINARY: SqlType = SqlType(913);
pub const BLOBLOCATOR: SqlType = SqlType(960);
pub const NBLOBLOCATOR: SqlType = SqlType(961);
pub const CLOBLOCATOR: SqlType = SqlType(964);
pub const NCLOBLOCATOR: SqlType = SqlType(965);
pub const DBCLOBLOCATOR: SqlType = SqlType(968);
pub const NDBCLOBLOCATOR: SqlType = SqlType(969);
pub const XML: SqlType = SqlType(988);
pub const NXML: SqlType = SqlType(989);
pub const DECFLOAT: SqlType = SqlType(996);
pub const NDECFLOAT: SqlType = SqlType(997);
pub const FAKEUDT: SqlType = SqlType(2000);
pub const FAKENUDT: SqlType = SqlType(2001);
pub const BOOLEAN: SqlType = SqlType(2436);
pub const NBOOLEAN: SqlType = SqlType(2437);
