use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use glob::glob;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use zstd::Decoder;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long = "output-dir", short = 'o')]
    output_dir: PathBuf,
    #[clap(long = "query-json", short = 'q')]
    query_json: PathBuf,
    #[clap(long = "input-folder", short = 'i', alias = "files-folder")]
    files_folder: String,
    #[clap(long = "search-management-file", short = 'm')]
    management_file: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Query {
    filename: String,
    expressions: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct Management {
    c_files: Vec<PathBuf>,
    c_lines: u64,
}

struct Output {
    files: Vec<BufWriter<File>>,
    management: Management,
    management_file: PathBuf,
}

fn search_line(line: &str, queries: &[AhoCorasick], does_match: &mut [bool]) {
    for (does_match, query) in does_match.iter_mut().zip(queries) {
        *does_match = query.is_match(line);
    }
}

fn search_file(
    management: &Management,
    file_path: &PathBuf,
    queries: &[Query],
    searchers: &[AhoCorasick],
    output_data: &Mutex<Output>,
) {
    if management.c_files.contains(file_path) {
        println!("Skipping file {} (completed)", file_path.display());
        return;
    }

    println!("Searching {}...", file_path.display());
    let now = std::time::Instant::now();

    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening {}: {e}", file_path.display());
            return;
        }
    };
    let mut reader = match Decoder::new(file) {
        Ok(r) => BufReader::new(r),
        Err(e) => {
            eprintln!("Error opening {}: {e}", file_path.display());
            return;
        }
    };

    let mut line_count = 0;
    let mut line_buf = String::new();
    let mut found_count = 0;
    // We'll be doing the line search a lot, and we don't know at compile-time how many
    // queries we'll have, so instead of allocating a new vector for each line we'll
    // pass one in and reset it for each line read.
    // Note that the order of these should match the order of `queries`.
    let mut does_match = vec![false; queries.len()];
    let mut matches: Vec<Vec<String>> = vec![Vec::new(); queries.len()];
    let mut match_count = 0;
    loop {
        line_buf.clear();
        does_match.fill(false);
        match reader.read_line(&mut line_buf) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading {}: {e}", file_path.display());
                return;
            }
        }

        search_line(&line_buf, searchers, &mut does_match);

        for (does_match, match_list) in does_match.iter().zip(&mut matches) {
            if *does_match {
                match_list.push(line_buf.clone());
                match_count += 1;
                found_count += 1;
            }
        }

        if match_count == 1000 {
            let mut lock = output_data.lock().unwrap();
            if write_matches(&matches, queries, &mut lock.files).is_err() {
                // Return here, so that it doesn't get marked as complete.
                return;
            }
            matches.iter_mut().for_each(|c| c.clear());
            match_count = 0;
        }

        line_count += 1;
    }

    let mut lock = output_data.lock().unwrap();

    if match_count > 0 && write_matches(&matches, queries, &mut lock.files).is_err() {
        // Return here, so that it doesn't get marked as complete.
        return;
    }

    // We've now finished searching this file, update the management.
    lock.management.c_files.push(file_path.clone());
    lock.management.c_lines += line_count;

    let elapsed = now.elapsed();
    println!("Took {elapsed:?} to search {line_count} lines, found {found_count} results",);

    // Now write out the management.
    let rendered = match serde_json::to_string_pretty(&lock.management) {
        Ok(r) => r,
        Err(_) => {
            eprintln!("Error rendering management file");
            return;
        }
    };

    if std::fs::write(&lock.management_file, &rendered).is_err() {
        eprintln!("Error writing management file");
    }
}

fn write_matches(
    matches: &[Vec<String>],
    queries: &[Query],
    output_files: &mut [BufWriter<File>],
) -> Result<(), ()> {
    for ((matches, query), output_file) in matches.iter().zip(queries).zip(output_files) {
        if matches.is_empty() {
            continue;
        }

        for match_ in matches {
            if output_file.write_all(match_.as_bytes()).is_err() {
                eprintln!("Error writing to {}", query.filename);
                return Err(());
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !Path::new(&args.files_folder).is_dir() {
        bail!("Error: files_folder must be a directory");
    }

    let glob_pattern = args.files_folder.clone() + "/**/*.zst";
    let zstd_files: Vec<_> = glob(&glob_pattern)
        .with_context(|| anyhow!("Error finding zst files"))?
        .collect::<Result<_, _>>()
        .with_context(|| anyhow!("Error finding zst files"))?;

    if zstd_files.is_empty() {
        eprintln!("No zst files found in `{}`", args.files_folder);
        return Ok(());
    }

    let query_file = std::fs::read_to_string(&args.query_json)
        .with_context(|| anyhow!("Error opening query file"))?;
    let queries: Vec<Query> =
        serde_json::from_str(&query_file).with_context(|| anyhow!("Error parsing query file"))?;

    let searchers: Vec<_> = queries
        .iter()
        .map(|q| {
            AhoCorasickBuilder::new()
                .ascii_case_insensitive(true)
                .build(&q.expressions)
        })
        .collect();

    std::fs::create_dir_all(&args.output_dir)
        .with_context(|| anyhow!("Error creating output directory"))?;

    let management = if args.management_file.exists() {
        let contents = std::fs::read_to_string(&args.management_file)
            .with_context(|| anyhow!("Error opening management file"))?;
        serde_json::from_str(&contents).with_context(|| anyhow!("Error parsing management file"))?
    } else {
        Management::default()
    };

    // Ensure the folder exists if the management path has a parent.
    if let Some(parent) = args.management_file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| anyhow!("Error creating parent directory for management file"))?;
    }

    let mut output_files = Vec::new();
    for query in &queries {
        let path = args.output_dir.join(&query.filename);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .with_context(|| anyhow!("Error creating output file {}", path.display()))?;
        output_files.push(BufWriter::new(file));
    }

    let output_files_mutex = Mutex::new(Output {
        files: output_files,
        management: management.clone(),
        management_file: args.management_file,
    });

    zstd_files.par_iter().for_each(|file_path| {
        search_file(
            &management,
            file_path,
            &queries,
            &searchers,
            &output_files_mutex,
        )
    });

    Ok(())
}
