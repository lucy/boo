use clap::Parser;
use rusqlite::{Connection, OpenFlags, Rows};
use std::error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::mem::{drop, swap};
use std::path::PathBuf;
use tempfile::NamedTempFile;

mod timefmt;

#[derive(Parser)]
#[clap(help_template = "{usage-heading} {usage}\n\n{all-args}")]
struct CLI {
    /// path to places.sqlite db
    #[clap(value_parser)]
    db: PathBuf,

    /// merge with previous export
    #[clap(value_parser, value_name = "FILE")]
    merge: Option<PathBuf>,

    /// output file
    #[clap(short, long, value_parser, value_name = "FILE")]
    output: Option<PathBuf>,

    /// output to input export
    #[clap(short, long, requires = "merge", conflicts_with = "output")]
    in_place: bool,
}

struct EntryBuf {
    str: String,
    pre: usize,
}

impl EntryBuf {
    fn new() -> Self { EntryBuf { str: String::new(), pre: 0 } }
    fn prefix(&self) -> &str { &self.str[..self.pre] }
}

fn db_next(r: &mut Rows, e: &mut EntryBuf) -> rusqlite::Result<bool> {
    let row = match r.next()? {
        Some(row) => row,
        None => return Ok(false),
    };
    let url = row.get_ref(0)?;
    let title = row.get_ref(1)?;
    let date: u64 = row.get(2)?;
    timefmt::usec(date, &mut e.str);
    e.str.push(' ');
    e.str.push_str(url.as_str()?);
    e.pre = e.str.len();
    if let Ok(title) = title.as_str() {
        e.str.push('\t');
        e.str.push_str(title);
    }
    e.str.push('\n');
    Ok(true)
}

fn file_next(r: &mut Option<impl BufRead>, e: &mut EntryBuf) -> io::Result<bool> {
    r.as_mut().map_or(Ok(false), |r| match r.read_line(&mut e.str)? {
        0 => Ok(false),
        _ => {
            e.pre = e.str.find('\t').unwrap_or(e.str.len() - 1);
            Ok(true)
        }
    })
}

struct Dedup<W> {
    e: EntryBuf,
    w: W,
}

impl<W: Write> Dedup<W> {
    fn new(w: W) -> Dedup<W> { Dedup { e: EntryBuf::new(), w } }

    fn put(&mut self, e: &mut EntryBuf) -> io::Result<()> {
        if e.prefix() != self.e.prefix() {
            self.w.write(e.str.as_bytes())?;
            swap(e, &mut self.e);
        }
        e.str.clear();
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let cli = CLI::parse();
    let c = Connection::open_with_flags(cli.db.as_path(), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut hs = c.prepare(
        r#"
        SELECT p.url, p.title, v.visit_date
        FROM moz_places p
        JOIN moz_historyvisits v
            ON p.id = v.place_id
        ORDER BY v.visit_date, p.url"#,
    )?;
    let mut hr = hs.query([])?;
    let mut tmp = None;
    let out = match cli.output {
        Some(ref p) => Box::new(File::create(p)?) as Box<dyn Write>,
        None if cli.in_place => {
            tmp = Some(NamedTempFile::new_in(cli.merge.as_ref().unwrap().parent().unwrap())?);
            Box::new(tmp.as_mut().unwrap())
        }
        None => Box::new(std::io::stdout().lock()),
    };
    let mut d = Dedup::new(BufWriter::with_capacity(64 * 1024, out));
    let mut f = cli.merge.as_ref().map(|p| File::open(p).map(BufReader::new)).transpose()?;
    let mut dbe = EntryBuf::new();
    let mut fie = EntryBuf::new();
    let mut dbn = db_next(&mut hr, &mut dbe)?;
    let mut fin = file_next(&mut f, &mut fie)?;
    while dbn || fin {
        if fin && (!dbn || fie.prefix() <= dbe.prefix()) {
            d.put(&mut fie)?;
            fin = file_next(&mut f, &mut fie)?;
        }
        if dbn && (!fin || dbe.prefix() < fie.prefix()) {
            d.put(&mut dbe)?;
            dbn = db_next(&mut hr, &mut dbe)?;
        }
    }
    drop(d);
    if let Some(f) = tmp {
        let (f, p) = f.keep()?;
        drop(f); // have to close file before renaming on windows
        std::fs::rename(p, cli.merge.as_ref().unwrap())?;
    };
    Ok(())
}
