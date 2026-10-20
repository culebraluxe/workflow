use std::fs::File;
use std::io::Read;

/// RFC 4122 UUID v4 from `/dev/urandom`. Neon `uuid` columns require this shape.
pub fn uuid_v4() -> String {
    let mut b = [0u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .expect("urandom");
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    )
}

pub fn millis_to_ts_sql() -> &'static str {
    "to_timestamp($TS::double precision / 1000.0)"
}
