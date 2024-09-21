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
use walkdir::WalkDir;

use lingua::Language::*;
use lingua::LanguageDetector;
use lingua::LanguageDetectorBuilder;
//most essential book details for dedupping
#[derive(Clone)]
struct Book {
    location: String, //path to the book
    size: i64,        //how many bytes large is the book
}

pub struct Scanner {
    dirs: Vec<String>,
    detector: LanguageDetector,
}

impl Scanner {
    pub fn new(dirs: Vec<String>) -> Self {
        Scanner {
            dirs,
            detector: LanguageDetectorBuilder::from_all_languages()
                .with_minimum_relative_distance(0.9)
                .build(),
        }
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
            let walker = WalkDir::new(&dir).into_iter();
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
        match self.detector.detect_language_of(
            bm.title.as_ref().unwrap_or(&"".to_string()).to_owned()
                + " "
                + bm.description.as_ref().unwrap_or(&"".to_string()),
        ) {
            Some(English) => true,
            Some(_) => false,
            None => true,
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
