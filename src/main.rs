use chrono::Datelike;
use chrono::NaiveDateTime;
use chrono::Timelike;
use clap::Parser;
use core::cmp::Ordering::*;
use rusqlite::{Connection, OpenFlags, Result, Rows};
use std::cmp::Ord;
use std::fs::File;
use std::io::{self, prelude::*, BufReader, BufWriter};
use std::mem::swap;
use std::path::PathBuf;

#[derive(Parser)]
struct CLI {
    /// path to places.sqlite db
    #[clap(value_parser)]
    db: PathBuf,
    /// previous export to merge with
    #[clap(value_parser)]
    file: Option<PathBuf>,
    /// output file
    #[clap(short, value_parser)]
    out: Option<PathBuf>,
}

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

fn format_usec(usec: u64, b: &mut String) {
    let secs = usec / 1_000_000;
    let nsec = (usec - secs * 1_000_000) * 1_000;
    let dt = NaiveDateTime::from_timestamp(secs as i64, nsec as u32);
    rfc3339(&dt, b);
}

fn db_next(r: &mut Rows, buf: &mut String) -> Result<bool> {
    let row = match r.next()? {
        Some(row) => row,
        None => return Ok(false),
    };
    let p_url = row.get_ref(1)?;
    let p_title = row.get_ref(2)?;
    let v_visit_date: u64 = row.get(7)?;
    format_usec(v_visit_date, buf);
    buf.push(' ');
    buf.push_str(p_url.as_str()?);
    if let Ok(title) = p_title.as_str() {
        buf.push('\t');
        buf.push_str(title);
    }
    buf.push('\n');
    Ok(true)
}

fn file_next(r: &mut Option<impl BufRead>, buf: &mut String) -> io::Result<bool> {
    r.as_mut().map_or(Ok(false), |r| match r.read_line(buf) {
        Ok(0) => Ok(false),
        Ok(_) => Ok(true),
        Err(e) => Err(e),
    })
}

struct Dedup<W> {
    l: bool,
    b: String,
    w: W,
}

fn get_prefix(s: &str) -> &str {
    if let Some(i) = s.find('\t') {
        return &s[..i];
    }
    return s;
}

impl<W: Write> Dedup<W> {
    fn put(&mut self, b: &mut String) {
        if get_prefix(b) != get_prefix(&self.b) {
            if self.l {
                self.w.write(self.b.as_bytes()).unwrap();
            }
            self.l = true;
            swap(b, &mut self.b);
        }
        b.clear();
    }
    fn end(&mut self) {
        if self.l {
            self.w.write(self.b.as_bytes()).unwrap();
        }
    }
}

fn main() -> Result<()> {
    let cli = CLI::parse();
    let c = Connection::open_with_flags(cli.db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut hs = c.prepare(
        r#"
        SELECT p.id, p.url, p.title, p.description, p.preview_image_url,
            v.id, v.from_visit, v.visit_date, v.visit_type
        FROM moz_places p
        JOIN moz_historyvisits v
            ON p.id = v.place_id
        ORDER BY v.visit_date, p.url"#,
    )?;
    let mut hr = hs.query([])?;
    let mut f = cli.file.map(|p| BufReader::new(File::open(p).unwrap()));
    let mut dbuf = String::new();
    let mut fbuf = String::new();
    let mut dbn = db_next(&mut hr, &mut dbuf)?;
    let mut fin = file_next(&mut f, &mut fbuf).unwrap();
    let mut d = Dedup {
        l: false,
        b: String::new(),
        w: BufWriter::with_capacity(
            256 * 1024,
            Box::new(match cli.out {
                Some(p) => Box::new(File::create(p).unwrap()) as Box<dyn Write>,
                None => Box::new(std::io::stdout().lock()),
            }),
        ),
    };
    while dbn || fin {
        if fin && !dbn {
            d.put(&mut fbuf);
            fin = file_next(&mut f, &mut fbuf).unwrap();
            continue;
        }
        if dbn && !fin {
            d.put(&mut dbuf);
            dbn = db_next(&mut hr, &mut dbuf)?;
            continue;
        }
        let c = get_prefix(&dbuf).cmp(get_prefix(&fbuf));
        if c >= Equal {
            d.put(&mut fbuf);
            fin = file_next(&mut f, &mut fbuf).unwrap();
        }
        if c <= Equal {
            d.put(&mut dbuf);
            dbn = db_next(&mut hr, &mut dbuf)?;
        }
    }
    d.end();
    Ok(())
}
