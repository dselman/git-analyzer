use std::fmt::Debug;
use std::{collections::HashMap, env};

use chrono::{DateTime, TimeZone, Utc};
use git2::{Error, Repository, Time};
use rusqlite::{Connection, Result};

use regex::Regex;

#[derive(Debug)]
pub struct FileInfo {
    path: String,
    status: String,
    added_lines: Option<i32>,
    removed_lines: Option<i32>,
}

#[derive(Debug)]
pub struct GitLogEntry {
    id: String,
    summary: String,
    author_name: String,
    author_email: String,
    author_when: DateTime<Utc>,
    files: Vec<FileInfo>,
}

#[derive(Debug)]
pub struct QueryResult {
    id: String,
    summary: String,
    author_name: String,
    author_email: String,
    author_when: DateTime<Utc>,
    path: String,
    status: String,
    added: Option<i32>,
    deleted: Option<i32>,
}

fn convert_git_time_to_datetime(git_time: &Time) -> DateTime<Utc> {
    Utc.timestamp(
        git_time.seconds() + i64::from(git_time.offset_minutes()) * 60,
        0,
    )
}

fn process_numberstats(
    diff: &git2::Diff,
    files_map: &HashMap<String, FileInfo>,
) -> Result<HashMap<String, FileInfo>, Error> {
    let mut result: HashMap<String, FileInfo> = HashMap::new();

    let re = Regex::new(r"^(\d+)[ ]+(\d+)[ ]+(\S+)$").unwrap();

    let stats = diff.stats()?;
    let format = git2::DiffStatsFormat::NUMBER;
    let buf = stats.to_buf(format, 80)?;
    let numberstats = std::str::from_utf8(&*buf)
        .unwrap_or_else(|_| "")
        .to_string();
    let lines = numberstats.trim().split("\n");

    for line in lines {
        let captures = re.captures(line);

        match captures {
            Some(caps) => {
                let added = caps.get(1).map_or("", |m| m.as_str());
                let removed = caps.get(2).map_or("", |m| m.as_str());
                let path = caps.get(3).map_or("", |m| m.as_str());

                let file_info = files_map.get(path);
                match file_info {
                    Some(fi) => {
                        result.insert(
                            path.to_string(),
                            FileInfo {
                                path: fi.path.to_string(),
                                status: fi.status.to_string(),
                                added_lines: Some(parse_int(added)),
                                removed_lines: Some(parse_int(removed)),
                            },
                        );
                    }
                    None => {
                    }
                }
            }
            None => {
            },
        }
    }
    Ok(result)
}

fn parse_int(input: &str) -> i32 {
    match input.parse() {
        Ok(number) => number,
        Err(_) => -1,
    }
}

fn get_diff_delta_status(delta: git2::DiffDelta) -> &str {
    let status_string: &str = match delta.status() {
        git2::Delta::Added => "Added",
        git2::Delta::Unmodified => "Unmodified",
        git2::Delta::Deleted => "Deleted",
        git2::Delta::Modified => "Modified",
        git2::Delta::Copied => "Copied",
        git2::Delta::Ignored => "Ignored",
        git2::Delta::Untracked => "Untracked",
        git2::Delta::Typechange => "Typechange",
        git2::Delta::Unreadable => "Unreadable",
        git2::Delta::Conflicted => "Conflicted",
        git2::Delta::Renamed => "Renamed",
    };
    status_string
}

pub fn walk_history(git_repo_path: &str) -> Result<Vec<GitLogEntry>, Error> {
    let repo = Repository::open(git_repo_path)?;
    let mut vec: Vec<GitLogEntry> = Vec::new();
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    let _ = revwalk.set_sorting(git2::Sort::TIME | git2::Sort::REVERSE);
    for rev in revwalk {
        let commit = repo.find_commit(rev?)?;
        let message = commit
            .summary_bytes()
            .unwrap_or_else(|| commit.message_bytes());
        let author_name = match commit.author().name() {
            None => "<none>".to_string(),
            Some(n) => n.to_string(),
        };
        let author_email = match commit.author().email() {
            None => "<none>".to_string(),
            Some(e) => e.to_string(),
        };

        // Ignore merge commits (2+ parents) because that's what 'git whatchanged' does.
        // Ignore commit with 0 parents (initial commit) because there's nothing to diff against
        let mut files_map: HashMap<String, FileInfo> = HashMap::new();

        if commit.parent_count() == 1 {
            let prev_commit = commit.parent(0)?;
            let tree = commit.tree()?;
            let prev_tree = prev_commit.tree()?;
            let diff = repo.diff_tree_to_tree(Some(&prev_tree), Some(&tree), None)?;

            for delta in diff.deltas() {
                let file_path = String::from(delta.new_file().path().unwrap().to_string_lossy());
                let s_slice: &str = &file_path[..];
                let status_string = get_diff_delta_status(delta);

                files_map.insert(
                    s_slice.to_string(),
                    FileInfo {
                        status: status_string.to_string(),
                        path: s_slice.to_string(),
                        added_lines: None,
                        removed_lines: None,
                    },
                );
            }
            let new_files_map = process_numberstats(&diff, &files_map).unwrap();
            for f in new_files_map.values() {
                files_map.insert( f.path.to_string(), FileInfo {
                    path: f.path.to_string(),
                    status: f.status.to_string(),
                    added_lines: f.added_lines,
                    removed_lines: f.removed_lines,
                });
            }
        }
        vec.push(GitLogEntry {
            id: commit.id().to_string(),
            summary: String::from_utf8_lossy(message).to_string(),
            author_name: author_name,
            author_email: author_email,
            author_when: convert_git_time_to_datetime(&commit.time()),
            files: Vec::from_iter(files_map.values().map(|f| FileInfo {
                path: f.path.clone(),
                status: f.status.clone(),
                added_lines: f.added_lines,
                removed_lines: f.removed_lines,
            })),
        });
    }

    return Ok(vec);
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let repo_path = &args[1];
    println!("Analyzing Git repository at {:?}", repo_path);

    let conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS commits (
            id	TEXT UNIQUE,
            summary	TEXT,
            author_name	TEXT,
            author_email	TEXT,
            author_when	DATETIME
            );",
        (), // empty list of parameters.
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS commit_files (
            id    TEXT,
            name  TEXT,
            status  TEXT,
            added INT,
            deleted INT
            );",
        (), // empty list of parameters.
    )?;

    let commits = walk_history(repo_path).unwrap();

    for commit in commits {
        let s_slice: &str = &commit.id[..];
        conn.execute(
            "INSERT INTO commits (id, summary, author_name, author_email, author_when) VALUES (?1, ?2, ?3, ?4, ?5)",
            (s_slice, commit.summary, commit.author_name, commit.author_email, commit.author_when),
        )?;
        for file in commit.files {
            conn.execute(
                "INSERT INTO commit_files (id, name, status, added, deleted) VALUES (?1, ?2, ?3, ?4, ?5)",
                (s_slice, file.path, file.status, file.added_lines, file.removed_lines),
            )?;
        }
    }

    let mut stmt = conn.prepare("SELECT commits.id, commits.summary, commits.author_name, commits.author_email, commits.author_when, commit_files.name, commit_files.status, commit_files.added, commit_files.deleted FROM commits INNER JOIN commit_files ON commits.id=commit_files.id")?;
    let commit_iter = stmt.query_map([], |row| {
        Ok(QueryResult {
            id: row.get(0)?,
            summary: row.get(1)?,
            author_name: row.get(2)?,
            author_email: row.get(3)?,
            author_when: row.get(4)?,
            path: row.get(5)?,
            status: row.get(6)?,
            added: row.get(7)?,
            deleted: row.get(8)?,
        })
    })?;

    for commit in commit_iter {
        let c = commit.unwrap();
        println!(
            "{:?}\t{:?}\t{:?}\t{:?}\t{:?}\t{:?}\t{:?}\t{:?}\t{:?}",
            c.id,
            c.author_when,
            c.author_name,
            c.author_email,
            c.path,
            c.status,
            c.added,
            c.deleted,
            c.summary
        );
    }
    Ok(())
}
