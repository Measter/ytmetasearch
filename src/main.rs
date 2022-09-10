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

#[derive(Debug, Default, Deserialize, Serialize)]
struct Management {
    c_files: Vec<PathBuf>,
    c_lines: u64,
}

fn search_line(line: &str, queries: &[AhoCorasick], does_match: &mut [bool]) {
    for (does_match, query) in does_match.iter_mut().zip(queries) {
        *does_match = query.is_match(line);
    }
}

fn search_file(
    management: &Management,
    mut sub_management: Management,
    file_path: &PathBuf,
    queries: &[Query],
    searchers: &[AhoCorasick],
    output_files_mutex: &Mutex<Vec<BufWriter<File>>>,
) -> Management {
    if management.c_files.contains(file_path) {
        println!("Skipping file {} (completed)", file_path.display());
        return sub_management;
    }

    println!("Searching {}...", file_path.display());
    let now = std::time::Instant::now();

    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error opening {}: {e}", file_path.display());
            return sub_management;
        }
    };
    let mut reader = match Decoder::new(file) {
        Ok(r) => BufReader::new(r),
        Err(e) => {
            eprintln!("Error opening {}: {e}", file_path.display());
            return sub_management;
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
                return sub_management;
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
            if write_matches(&matches, queries, output_files_mutex).is_err() {
                // Return here, so that it doesn't get marked as complete.
                return sub_management;
            }
            matches.iter_mut().for_each(|c| c.clear());
            match_count = 0;
        }

        line_count += 1;
    }

    if match_count > 0 && write_matches(&matches, queries, output_files_mutex).is_err() {
        // Return here, so that it doesn't get marked as complete.
        return sub_management;
    }

    // We've now finished searching this file, update the management.
    sub_management.c_files.push(file_path.clone());
    sub_management.c_lines += line_count;

    let elapsed = now.elapsed();
    println!("Took {elapsed:?} to search {line_count} lines, found {found_count} results",);

    sub_management
}

fn write_matches(
    matches: &[Vec<String>],
    queries: &[Query],
    output_files: &Mutex<Vec<BufWriter<File>>>,
) -> Result<(), ()> {
    let mut output_files = output_files.lock().unwrap();
    for ((matches, query), output_file) in matches.iter().zip(queries).zip(&mut *output_files) {
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

    let mut management = if args.management_file.exists() {
        let contents = std::fs::read_to_string(&args.management_file)
            .with_context(|| anyhow!("Error opening management file"))?;
        serde_json::from_str(&contents).with_context(|| anyhow!("Error parsing management file"))?
    } else {
        Management::default()
    };

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

    let output_files_mutex = Mutex::new(output_files);

    let new_management = zstd_files
        .par_iter()
        .fold(Management::default, |sub_management, file_path| {
            search_file(
                &management,
                sub_management,
                file_path,
                &queries,
                &searchers,
                &output_files_mutex,
            )
        })
        .reduce(Management::default, |mut sum, cur| {
            sum.c_files.extend(cur.c_files);
            sum.c_lines += cur.c_lines;
            sum
        });

    // Merge the new management with the old.
    management.c_files.extend(new_management.c_files);
    management.c_lines += new_management.c_lines;

    // Now write out the management.
    let rendered = serde_json::to_string_pretty(&management)
        .with_context(|| anyhow!("Error rendering management JSON"))?;

    // Ensure the folder exists if the path has a parent.
    if let Some(parent) = args.management_file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| anyhow!("Error creating parent directory for management file"))?;
    }

    std::fs::write(&args.management_file, &rendered)
        .with_context(|| anyhow!("Error writing management file"))?;

    Ok(())
}
