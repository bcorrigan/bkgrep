use crate::BookMetadata;
use epub::doc::EpubDoc;
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::process;
use std::sync::RwLock;
use walkdir::{DirEntry, WalkDir};

use lingua::Language::*;
use lingua::LanguageDetector;
use lingua::LanguageDetectorBuilder;
use rand::RngExt;
use scraper::html::Html;
//most essential book details for dedupping
#[derive(Clone)]
struct Book {
    location: String, //path to the book
    size: i64,        //how many bytes large is the book
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

pub struct Scanner {
    dirs: Vec<String>,
    detector: Option<LanguageDetector>,
}

impl Scanner {
    pub fn new(dirs: Vec<String>, detect_lang: bool) -> Self {
        let detector = if detect_lang {
            Some(
                LanguageDetectorBuilder::from_all_languages()
                    .with_minimum_relative_distance(0.9)
                    .build(),
            )
        } else {
            None
        };

        Scanner { dirs, detector }
    }

    pub fn scan_dirs(&self) -> Result<(), Box<dyn std::error::Error>> {
        for directory in &self.dirs {
            if !Path::new(&directory).exists() {
                eprintln!("Directory {} does not exist.", &directory);
                process::exit(3);
            }
        }

        // all books seen so far. For now store the location and fngers crossed don't run out of memory
        let seen_books: RwLock<HashMap<i64, Vec<Book>>> = std::sync::RwLock::new(HashMap::new());
        let mut book_batch = vec![];

        for dir in &self.dirs {
            let walker = WalkDir::new(&dir)
                .into_iter()
                .filter_entry(|e| !is_hidden(e));
            for entry in walker {
                match entry {
                    Ok(l) => {
                        if l.path().display().to_string().ends_with(".epub")
                            && l.file_type().is_file()
                        {
                            book_batch.push(l.path().display().to_string());

                            if book_batch.len() % 10000 == 0 {
                                self.process_batch(&seen_books, &book_batch);
                                book_batch.clear();
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Unrecoverable error while scanning books:{}", e);
                        process::exit(1);
                    }
                }
            }
            if book_batch.len() > 0 {
                self.process_batch(&seen_books, &book_batch);
                book_batch.clear();
            }
        }

        let final_books = seen_books.into_inner().unwrap();
        Self::report_dups(&final_books);

        Ok(())
    }

    fn process_batch(
        &self,
        seen_books: &RwLock<HashMap<i64, Vec<Book>>>,
        book_batch: &Vec<String>,
    ) {
        book_batch
            .par_iter()
            .for_each(|book_path| match parse_epub(book_path) {
                Ok(bm) => {
                    if self.is_english(&bm) {
                        let new_bk = Book {
                            location: book_path.clone(),
                            size: bm.filesize,
                        };
                        seen_books
                            .write()
                            .unwrap()
                            .entry(bm.id)
                            .or_insert_with(Vec::new)
                            .push(new_bk);
                    } else {
                        println!("FRN:{}", book_path);
                    }
                }
                Err(err) => {
                    eprintln!("Error with {}: {:?}", book_path, err);
                    println!("ERROR:{}", book_path);
                }
            });
    }

    //the potential issue here is there's a difference between "yes tis is definitely english" and "this is definitely NOT english"
    //books with eg ambiguous title and no description won't be detected!
    //That's why we must detect using using ALL languages
    fn is_english(&self, bm: &BookMetadata) -> bool {
        if let Some(detector) = &self.detector {
            if bm.description.as_ref().is_some_and(|s| s.len() > 50) {
                match detector.detect_language_of(
                    bm.title.as_ref().unwrap_or(&"".to_string()).to_owned()
                        + " "
                        + bm.description.as_ref().unwrap_or(&"".to_string()),
                ) {
                    Some(English) => true,
                    Some(_) => false,
                    None => true,
                }
            } else {
                //not enough information to be sure - inspect inside the book at a random point
                //this is all prettyugly and hurried :/
                let mut doc = EpubDoc::new(&bm.file).unwrap();
                let mut content = String::new();
                add_content(&mut doc, &mut content);
                add_content(&mut doc, &mut content);
                add_content(&mut doc, &mut content);
                let mut cleaned = String::new();

                let fragdoc = Html::parse_fragment(&content);
                for node in fragdoc.tree {
                    if let scraper::node::Node::Text(text) = node {
                        cleaned.push_str(&text.text);
                    }
                }

                match detector.detect_language_of(cleaned) {
                    Some(English) => true,
                    Some(_) => false,
                    None => true,
                }
            }
        } else {
            true
        }
    }

    // Books are hashed by title and creator alone, so a single id may have many books behind it
    // (same author/title, different publishers/editions). We aggressively dedupe within a 5% size
    // band: the largest book in each band is kept, the rest are reported as DUP. Books outside the
    // band are treated as a substantially different edition and retained separately.
    //
    // We sort all books for an id by size ascending and walk left-to-right, greedily growing a
    // band while the next book is within 5% of the band's smallest member. This avoids the
    // pairwise-swap pathology where a chain of close-sized books all collapse to the largest one.
    fn report_dups(seen_books: &HashMap<i64, Vec<Book>>) {
        for dups in seen_books.values() {
            if dups.len() <= 1 {
                continue;
            }
            let mut sorted: Vec<&Book> = dups.iter().collect();
            sorted.sort_by_key(|b| b.size);

            let mut i = 0;
            while i < sorted.len() {
                let band_max = (sorted[i].size as f64 * 1.05) as i64;
                let mut j = i;
                let mut max_idx = i;
                while j < sorted.len() && sorted[j].size <= band_max {
                    if sorted[j].size > sorted[max_idx].size {
                        max_idx = j;
                    }
                    j += 1;
                }
                for k in i..j {
                    if k != max_idx {
                        println!("DUP:{}", sorted[k].location);
                    }
                }
                i = j;
            }
        }
    }
}

fn add_content(doc: &mut EpubDoc<std::io::BufReader<File>>, content: &mut String) {
    let rand_page = rand::rng().random_range(0..doc.get_num_pages());
    doc.set_current_page(rand_page);
    content.push_str(" ");
    content.push_str(
        doc.get_current_str()
            .unwrap_or(("".to_string(), "".to_string()))
            .0
            .as_ref(),
    );
}
fn parse_epub(book_loc: &str) -> Result<BookMetadata, Box<dyn Error>> {
    let doc = EpubDoc::new(&book_loc)?;
    let metadata = fs::metadata(&book_loc)?;

    let file = match Path::new(&book_loc).canonicalize() {
        Ok(f) => f.display().to_string(),
        Err(e) => {
            eprintln!("Could not canonicalize {}", &e);
            return Err(Box::new(e));
        }
    };

    let mut bm = BookMetadata {
        id: 0i64,
        title: get_first_fd("title", &doc.metadata),
        description: get_first_fd("description", &doc.metadata),
        creator: get_first_fd("creator", &doc.metadata).map(unmangle_creator),
        file,
        filesize: metadata.len() as i64,
    };

    bm.id = bm.hash_md();
    Ok(bm)
}

fn get_first_fd(mdfield: &str, md: &HashMap<String, Vec<String>>) -> Option<String> {
    md.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(mdfield))
        .and_then(|(_, v)| v.first().cloned())
}

//Attempt to unmangle author names to be consistent
fn unmangle_creator(creator: String) -> String {
    let unspaced_creator = creator.split_whitespace().join(" ");
    if unspaced_creator.matches(',').count() == 1 {
        let parts: Vec<&str> = unspaced_creator.split(',').collect();
        return format!("{} {}", parts[1].trim(), parts[0].trim());
    }
    unspaced_creator
}

#[test]
fn test_unmangle() {
    let lovecraft = "H.P. Lovecraft".to_string();
    assert_eq!(lovecraft, unmangle_creator(lovecraft.clone()));
    assert_eq!(lovecraft, unmangle_creator("Lovecraft, H.P.".to_string()));
    assert_eq!(lovecraft, unmangle_creator("Lovecraft,  H.P. ".to_string()));
    assert_eq!(lovecraft, unmangle_creator("H.P.  Lovecraft".to_string()));
    assert_eq!(
        lovecraft,
        unmangle_creator("H.P. \t  Lovecraft".to_string())
    );
    assert_eq!(
        lovecraft,
        unmangle_creator(" H.P.\t \tLovecraft ".to_string())
    );
}
