mod scanner;
use clap::Parser;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::process;
pub struct BookMetadata {
    id: i64,
    title: Option<String>,
    description: Option<String>,
    creator: Option<String>,
    file: String,
    filesize: i64,
}

impl BookMetadata {
    pub fn add_counts(val: &Option<String>, counts: &mut HashMap<String, u32>) {
        if let Some(cat) = val {
            counts.insert(cat.to_string(), counts.get(cat).unwrap_or(&0) + 1);
        }
    }

    pub fn hash_md(&self) -> i64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish() as i64
    }
}

impl Hash for BookMetadata {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        (&self.title, &self.creator).hash(state);
    }
}

/// Find epub that match specific patterns (or not).
/// The file locations of epubs that match are written to std out.
/// Intended to allow scanning a collection of epubs and listing all the duplicate and foreign epubs
/// which can then be deleted.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Find epubs written in a foreign language ie. not english
    #[arg(short, long, action)]
    find_foreign: bool,

    /// Find epubs in these directories - directories are scanned in given order
    #[arg(long, default_value = ".", num_args=1.., value_parser)]
    dir: Vec<String>,

    /// Find epubs which are duplicates. Epubs with the same author & title are considered identical, however books with same author and title and a 5% size variance are retained seperately.
    #[arg(short, long, action)]
    dups: bool,
}

fn main() {
    let cli = Cli::parse();
    let scanner = scanner::Scanner::new(cli.dir, cli.find_foreign);
    if let Err(e) = scanner.scan_dirs() {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
