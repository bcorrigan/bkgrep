use crate::BookMetadata;
use epub::doc::EpubDoc;
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::process;
use std::sync::RwLock;
use std::time::SystemTime;
use walkdir::{DirEntry, WalkDir};

use lingua::Language::*;
use lingua::LanguageDetector;
use lingua::LanguageDetectorBuilder;
use rand::prelude::*;
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
        let seen_books: RwLock<HashMap<i64, Book>> = std::sync::RwLock::new(HashMap::new());
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

        Ok(())
    }

    fn process_batch(&self, seen_books: &RwLock<HashMap<i64, Book>>, book_batch: &Vec<String>) {
        book_batch
            .par_iter()
            .map(|book_path| match parse_epub(book_path) {
                Ok(bm) => {
                    if self.is_english(&bm) {
                        let new_bk = Book {
                            location: book_path.clone(),
                            size: bm.filesize,
                        };
                        if !seen_books.read().unwrap().contains_key(&bm.id) {
                            seen_books.write().unwrap().insert(bm.id, new_bk);
                            Some(bm)
                        } else {
                            //DUPLICATE DETECTED
                            let seen_unlocked = seen_books.read().unwrap();
                            let old_bk = seen_unlocked.get(&bm.id).unwrap().clone();
                            drop(seen_unlocked);
                            if Self::better_dup(&old_bk, &new_bk) {
                                println!("DUP:{}", old_bk.location);
                                seen_books.write().unwrap().insert(bm.id, new_bk);
                            } else {
                                println!("DUP:{}", new_bk.location);
                            }

                            None
                        }
                    } else {
                        println!("FRN:{}", book_path);
                        None
                    }
                }
                Err(err) => {
                    eprintln!("Error with {}: {:?}", book_path, err);
                    println!("ERROR:{}", book_path);
                    None
                }
            })
            .filter(|bmo| bmo.is_some())
            .map(|bms| bms.unwrap())
            .collect::<Vec<BookMetadata>>();
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
                let mut tref = String::new();

                let fragdoc = Html::parse_fragment(&content);
                for node in fragdoc.tree {
                    cleaned.push_str(match node {
                        scraper::node::Node::Text(text) => {
                            tref = text.text.to_string();
                            &tref
                        }
                        _ => "",
                    });
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

    fn better_dup(old: &Book, new: &Book) -> bool {
        if new.size > old.size {
            true
        } else {
            false
        }
    }
}

fn add_content(doc: &mut EpubDoc<std::io::BufReader<File>>, content: &mut String) {
    let rand_page = rand::thread_rng().gen_range(0..doc.get_num_pages());
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
    let mut doc = EpubDoc::new(&book_loc)?;
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
        publisher: get_first_fd("publisher", &doc.metadata),
        creator: get_first_fd("creator", &doc.metadata).map(unmangle_creator),
        file,
        filesize: metadata.len() as i64,
    };

    bm.id = bm.hash_md();
    Ok(bm)
}

fn get_first_fd(mdfield: &str, md: &HashMap<String, Vec<String>>) -> Option<String> {
    match md.get(mdfield) {
        Some(vec) => Some(vec.get(0).unwrap().clone()),
        None => None,
    }
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
