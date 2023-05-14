use rusqlite::{Connection, Result};
use chrono::naive::NaiveDate;
use chrono::{DateTime, Utc, Datelike};
use chrono::offset::TimeZone;
use git2::{Repository, Error, Time, Delta, Diff};


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
        let authorName = match commit.author().name() {
            None => "<none>".to_string(),
            Some(n) => {
                n.to_string()
            },
        };
        let authorEmail = match commit.author().email() {
            None => "<none>".to_string(),
            Some(e) => {
                e.to_string()
            },
        };
        
        vec.push( Commit {
            id: commit.id().to_string(),
            summary:  String::from_utf8_lossy(message).to_string(),
            author_name: authorName,
            author_email: authorEmail,
            author_when: convert_git_time_to_datetime(&commit.time())
        });
    }
    return Ok(vec);
}

fn main() -> Result<()> {

    let _ = walk_history(".");

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

    let commits = walk_history(".").unwrap();

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