use chrono::{Datelike, NaiveDateTime, Timelike};

fn itoa(buf: &mut String, mut i: u32, mut w: i32) {
    let mut b = [0u8; 4];
    let mut bp = b.len();
    while i > 0 || w > 0 {
        w -= 1;
        bp -= 1;
        b[bp] = b'0' + (i % 10) as u8;
        i /= 10;
    }
    buf.push_str(unsafe { std::str::from_utf8_unchecked(&b[bp..]) });
}

fn rfc3339(dt: &NaiveDateTime, b: &mut String) {
    let d = dt.date();
    itoa(b, d.year() as u32, 4);
    b.push('-');
    itoa(b, d.month(), 2);
    b.push('-');
    itoa(b, d.day(), 2);
    b.push(' ');
    let t = dt.time();
    itoa(b, t.hour(), 2);
    b.push(':');
    itoa(b, t.minute(), 2);
    b.push(':');
    itoa(b, t.second(), 2);
    b.push('.');
    itoa(b, t.nanosecond() / 1_000_000, 3);
    b.push('Z');
}

pub fn usec(usec: u64, b: &mut String) {
    let secs = usec / 1_000_000;
    let nsec = (usec - secs * 1_000_000) * 1_000;
    let dt = NaiveDateTime::from_timestamp(secs as i64, nsec as u32);
    rfc3339(&dt, b);
}
