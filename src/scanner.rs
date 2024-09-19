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
use std::time::SystemTime;
use walkdir::WalkDir;
pub fn scan_dirs(dirs: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    for directory in &dirs {
        if !Path::new(&directory).exists() {
            eprintln!("Directory {} does not exist.", &directory);
            process::exit(3);
        }
    }

    let seen_bookids = std::sync::RwLock::new(HashSet::new());
    let mut processed: u64 = 0;
    let mut book_batch = vec![];

    for dir in &dirs {
        let walker = WalkDir::new(&dir).into_iter();
        for entry in walker {
            match entry {
                Ok(l) => {
                    if l.path().display().to_string().ends_with(".epub") && l.file_type().is_file()
                    {
                        book_batch.push(l.path().display().to_string());

                        processed += 1;

                        if processed % 10000 == 0 || processed >= total_books {
                            let bms: Vec<BookMetadata> = book_batch
                                .par_iter()
                                .map(|book_path| match parse_epub(book_path) {
                                    Ok(bm) => {
                                        if !seen_bookids.read().unwrap().contains(&bm.id) {
                                            seen_bookids.write().unwrap().insert(bm.id);
                                            Some(bm)
                                        } else {
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
                                .collect();
                        }
                        book_batch.clear();
                    }
                }
                Err(e) => {
                    eprintln!("Unrecoverable error while scanning books:{}", e);
                    process::exit(1);
                }
            }
        }
    }

    Ok(())
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
