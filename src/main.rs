use rusqlite::{Connection, Result};
use chrono::naive::NaiveDate;
use chrono::{DateTime, Utc, Datelike};
use chrono::offset::TimeZone;
use git2::{Repository, Error, Time, Delta, Diff};


#[derive(Debug)]
struct Commit {
    id: String,
    summary: String,
    author_name: String,
    author_email: String,
    author_when: NaiveDate,
}

fn convert_git_time_to_datetime(git_time: &Time) -> DateTime<Utc> {
    Utc.timestamp(git_time.seconds() + i64::from(git_time.offset_minutes()) * 60, 0)
}

pub fn walk_history(git_repo_path: &str) -> Result<(), Error> {
    let repo = Repository::open(git_repo_path)?;
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    let _ = revwalk.set_sorting(git2::Sort::TIME | git2::Sort::REVERSE);
    for rev in revwalk {
        let commit = repo.find_commit(rev?)?;
        let message = commit.summary_bytes().unwrap_or_else(|| commit.message_bytes());
        println!("{}\t{}", commit.id(), String::from_utf8_lossy(message));
    }
    Ok(())
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

    let date_str = "2020-04-12";
    let naive_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();

    let test = Commit {
        id: "0".to_string(),
        summary: "This is a test".to_string(),
        author_name: "Dan Selman".to_string(),
        author_email: "danscode@selman,org".to_string(),
        author_when: naive_date
    };
    conn.execute(
        "INSERT INTO commits (id, summary, author_name, author_email, author_when) VALUES (?1, ?2, ?3, ?4, ?5)",
        (&test.id, &test.summary, &test.author_name, &test.author_email, &test.author_when),
    )?;

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