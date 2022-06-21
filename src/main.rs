use clap::Parser;
use core::cmp::Ordering::*;
use rusqlite::{Connection, OpenFlags, Rows};
use std::cmp::Ord;
use std::error;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter};
use std::mem::swap;
use std::path::PathBuf;
use tempfile::NamedTempFile;

mod timefmt;

#[derive(Parser)]
#[clap(help_template("{usage-heading} {usage}\n\n{all-args}"))]
struct CLI {
    /// path to places.sqlite db
    #[clap(value_parser)]
    db: PathBuf,
    /// previous export to merge with
    #[clap(value_parser, value_name = "FILE")]
    merge: Option<PathBuf>,
    /// output file
    #[clap(short, long, value_parser, value_name = "FILE")]
    output: Option<PathBuf>,
    /// overwrite merged export file
    #[clap(short, long, requires = "merge", conflicts_with = "output")]
    in_place: bool,
}

struct Entry {
    str: String,
    pre: usize,
}

impl Entry {
    fn new() -> Self { Entry { str: String::new(), pre: 0 } }
    fn prefix(&self) -> &str { &self.str[..self.pre] }
}

fn db_next(r: &mut Rows, e: &mut Entry) -> rusqlite::Result<bool> {
    let row = match r.next()? {
        Some(row) => row,
        None => return Ok(false),
    };
    let p_url = row.get_ref(1)?;
    let p_title = row.get_ref(2)?;
    let v_visit_date: u64 = row.get(7)?;
    timefmt::usec(v_visit_date, &mut e.str);
    e.str.push(' ');
    e.str.push_str(p_url.as_str()?);
    e.pre = e.str.len();
    if let Ok(title) = p_title.as_str() {
        e.str.push('\t');
        e.str.push_str(title);
    }
    e.str.push('\n');
    Ok(true)
}

fn file_next(r: &mut Option<impl BufRead>, e: &mut Entry) -> io::Result<bool> {
    r.as_mut().map_or(Ok(false), |r| match r.read_line(&mut e.str) {
        Ok(0) => Ok(false),
        Err(e) => Err(e),
        Ok(_) => {
            e.pre = e.str.find('\t').unwrap_or(e.str.len() - 1);
            Ok(true)
        }
    })
}

struct Dedup<W> {
    l: bool,
    e: Entry,
    w: W,
}

impl<W: Write> Dedup<W> {
    fn new(w: W) -> Dedup<W> { Dedup { l: false, e: Entry::new(), w } }

    fn put(&mut self, e: &mut Entry) -> io::Result<()> {
        if e.prefix() != self.e.prefix() {
            if self.l {
                self.w.write(self.e.str.as_bytes())?;
            }
            self.l = true;
            swap(e, &mut self.e);
        }
        e.str.clear();
        Ok(())
    }

    fn end(&mut self) -> io::Result<()> {
        if self.l {
            self.w.write(self.e.str.as_bytes())?;
        }
        Ok(())
    }
}

fn run(cli: &CLI, out: impl Write) -> Result<(), Box<dyn error::Error>> {
    let c = Connection::open_with_flags(cli.db.as_path(), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
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
    let mut f = cli.merge.as_ref().map(|p| File::open(p).map(BufReader::new)).transpose()?;
    let mut dbe = Entry::new();
    let mut fie = Entry::new();
    let mut dbn = db_next(&mut hr, &mut dbe)?;
    let mut fin = file_next(&mut f, &mut fie)?;
    let mut d = Dedup::new(BufWriter::with_capacity(256 * 1024, out));
    while dbn || fin {
        if fin && !dbn {
            d.put(&mut fie)?;
            fin = file_next(&mut f, &mut fie).unwrap();
            continue;
        }
        if dbn && !fin {
            d.put(&mut dbe)?;
            dbn = db_next(&mut hr, &mut dbe)?;
            continue;
        }
        let c = dbe.prefix().cmp(fie.prefix());
        if c >= Equal {
            d.put(&mut fie)?;
            fin = file_next(&mut f, &mut fie).unwrap();
        }
        if c <= Equal {
            d.put(&mut dbe)?;
            dbn = db_next(&mut hr, &mut dbe)?;
        }
    }
    d.end()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let cli = CLI::parse();
    let mut tmp = None;
    run(
        &cli,
        match cli.output {
            Some(ref p) => Box::new(File::create(p)?) as Box<dyn Write>,
            None if cli.in_place => {
                let f = NamedTempFile::new_in(cli.merge.as_ref().unwrap().parent().unwrap())?;
                tmp = Some(f);
                Box::new(tmp.as_mut().unwrap())
            }
            None => Box::new(std::io::stdout().lock()),
        },
    )?;
    if let Some(f) = tmp {
        let (f, p) = f.keep()?;
        std::mem::drop(f);
        std::fs::rename(p, cli.merge.as_ref().unwrap())?;
    };
    Ok(())
}
