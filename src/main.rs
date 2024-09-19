use clap::{Parser, Subcommand};
mod scanner;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::error::Error;
use std::hash::{Hash, Hasher};

pub struct BookMetadata {
    id: i64,
    title: Option<String>,
    description: Option<String>,
    publisher: Option<String>,
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
        (&self.title, &self.publisher, &self.creator).hash(state);
    }
}

/// Find epub that match specific patterns (or not).
/// The file locations of epubs that match are written to std out
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Find epubs written in these given languages only
    #[arg(short, long, default_value = ".", num_args=1.., value_parser)]
    langs: Vec<String>,

    /// Find epubs in these directories - directories are scanned in given order
    #[arg(short, long, default_value = ".", num_args=1.., value_parser)]
    dirs: Vec<String>,

    /// FInd epubs which are duplicates. Only first and subsequent duplicates are reported
    #[arg(short, long, action)]
    dups: bool,
}

fn main() {
    println!("Hello, world!");
}
