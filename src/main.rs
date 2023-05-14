use std::env;
use std::{cmp::max, collections::HashMap, path::PathBuf};
use chrono::{DateTime, Utc, TimeZone};
use rusqlite::{Connection, Result};
use git2::{Repository, Error, Time};


#[derive(Debug)]
pub struct Commit {
    id: String,
    summary: String,
    author_name: String,
    author_email: String,
    author_when: DateTime<Utc>,
}

fn convert_git_time_to_datetime(git_time: &Time) -> DateTime<Utc> {
    Utc.timestamp(git_time.seconds() + i64::from(git_time.offset_minutes()) * 60, 0)
}

pub fn walk_history(git_repo_path: &str) -> Result<Vec<Commit>, Error> {
    let repo = Repository::open(git_repo_path)?;
    let mut vec: Vec<Commit> = Vec::new();
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
        if commit.parent_count() == 1 {
            let prev_commit = commit.parent(0)?;
            let tree = commit.tree()?;
            let prev_tree = prev_commit.tree()?;
            let diff= repo.diff_tree_to_tree(Some(&prev_tree), Some(&tree), None)?;
            for delta in diff.deltas() {
                let file_path = delta.new_file().path().unwrap();
                let file_mod_time = commit.time();
                let unix_time = file_mod_time.seconds();
                println!("File path {:?} Time: {:?} Status {:?} New file: {:?} Old file: {:?}", file_path, unix_time, delta.status(), delta.new_file(), delta.old_file());
            }
        }
        
        vec.push( Commit {
            id: commit.id().to_string(),
            summary:  String::from_utf8_lossy(message).to_string(),
            author_name: author_name,
            author_email: author_email,
            author_when: convert_git_time_to_datetime(&commit.time())
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
            id    TEXT UNIQUE,
            name  TEXT,
            added INT,
            deleted INT
            );",
        (), // empty list of parameters.
    )?;

    let commits = walk_history(repo_path).unwrap();

    for commit in commits {
        conn.execute(
            "INSERT INTO commits (id, summary, author_name, author_email, author_when) VALUES (?1, ?2, ?3, ?4, ?5)",
            (commit.id, commit.summary, commit.author_name, commit.author_email, commit.author_when),
        )?;    
    }

    let mut stmt = conn.prepare("SELECT id, summary, author_name, author_email, author_when FROM commits")?;
    let commit_iter = stmt.query_map([], |row| {
        Ok(Commit {
            id: row.get(0)?,
            summary: row.get(1)?,
            author_name: row.get(2)?,
            author_email: row.get(3)?,
            author_when: row.get(4)?
        })
    })?;

    for commit in commit_iter {
        println!("Found commit {:?}", commit.unwrap());
    }
    Ok(())
}