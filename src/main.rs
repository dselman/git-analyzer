use std::env;
use std::fmt::Debug;

use chrono::{DateTime, Utc, TimeZone};
use rusqlite::{Connection, Result};
use git2::{Repository, Error, Time};

#[derive(Debug)]
pub struct FileInfo {
    path: String,
    status: String,
    added_lines: i32,
    removed_lines: i32,
}

#[derive(Debug)]
pub struct GitLogEntry {
    id: String,
    summary: String,
    author_name: String,
    author_email: String,
    author_when: DateTime<Utc>,
    files: Vec<FileInfo>
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
    added: i32,
    deleted: i32,
}

fn convert_git_time_to_datetime(git_time: &Time) -> DateTime<Utc> {
    Utc.timestamp(git_time.seconds() + i64::from(git_time.offset_minutes()) * 60, 0)
}

fn get_numberstats(diff: &git2::Diff) -> Result<String, Error> {
    let stats = diff.stats()?;
    let format = git2::DiffStatsFormat::NUMBER;
    let buf = stats.to_buf(format, 80)?;
    return Ok(std::str::from_utf8(&*buf).unwrap().to_string());
}

pub fn walk_history(git_repo_path: &str) -> Result<Vec<GitLogEntry>, Error> {
    let repo = Repository::open(git_repo_path)?;
    let mut vec: Vec<GitLogEntry> = Vec::new();
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    let _ = revwalk.set_sorting(git2::Sort::TIME | git2::Sort::REVERSE);
    for rev in revwalk {
        let commit = repo.find_commit(rev?)?;
        let message = commit.summary_bytes().unwrap_or_else(|| commit.message_bytes());
        let author_name = match commit.author().name() {
            None => "<none>".to_string(),
            Some(n) => {
                n.to_string()
            },
        };
        let author_email = match commit.author().email() {
            None => "<none>".to_string(),
            Some(e) => {
                e.to_string()
            },
        };

        // Ignore merge commits (2+ parents) because that's what 'git whatchanged' does.
        // Ignore commit with 0 parents (initial commit) because there's nothing to diff against
        let mut files: Vec<FileInfo> = Vec::new();
        if commit.parent_count() == 1 {
            let prev_commit = commit.parent(0)?;
            let tree = commit.tree()?;
            let prev_tree = prev_commit.tree()?;
            let diff= repo.diff_tree_to_tree(Some(&prev_tree), Some(&tree), None)?;
            let numberstats = get_numberstats(&diff).unwrap_or_else(|_| "<none>".to_string());
            print!("{}", numberstats);

            let lines = numberstats.trim().split("\n");
            for line in lines {
                let parts = line.split("      ");
                let mut it = parts.into_iter();
                let added = it.next().unwrap_or_default().trim().parse::<i32>().unwrap_or_default();
                let removed = it.next().unwrap_or_default().trim().parse::<i32>().unwrap_or_default();
                let path = it.next().unwrap_or_default().trim();
            
                files.push( FileInfo {
                    path: path.to_string(),
                    status: "TODO".to_string(),
                    added_lines: added,
                    removed_lines: removed
                });
            }

            // for delta in diff.deltas() {
            //     let file_path = delta.new_file().path().unwrap();
            //     // let file_mod_time = commit.time();
            //     let status_string = match delta.status() {
            //         git2::Delta::Added => "Added".to_string(),
            //         git2::Delta::Unmodified => "Unmodified".to_string(),
            //         git2::Delta::Deleted => "Deleted".to_string(),
            //         git2::Delta::Modified => "Modified".to_string(),
            //         git2::Delta::Copied => "Copied".to_string(),
            //         git2::Delta::Ignored => "Ignored".to_string(),
            //         git2::Delta::Untracked => "Untracked".to_string(),
            //         git2::Delta::Typechange => "Typechange".to_string(),
            //         git2::Delta::Unreadable => "Unreadable".to_string(),
            //         git2::Delta::Conflicted => "Conflicted".to_string(),
            //         git2::Delta::Renamed => "Renamed".to_string(),
            //     };
            // }
        }
        
        vec.push( GitLogEntry {
            id: commit.id().to_string(),
            summary:  String::from_utf8_lossy(message).to_string(),
            author_name: author_name,
            author_email: author_email,
            author_when: convert_git_time_to_datetime(&commit.time()),
            files,
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
        println!("Found commit {:?}", commit.unwrap());
    }
    Ok(())
}