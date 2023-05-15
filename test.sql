-- Most touched file
-- This shows how many commits reference a specific file, 
-- so we can see which is the most active.
select count(*), name from commits, commit_files
where commits.id = commit_files.id
group by name order by count(*) desc limit 20;

-- Most changed files in a time frame
-- Lets look to see which files have changed the most in a certain time frame. 
-- This gives you an idea of where the activity of the project has been focused.
select count(*) as commits, sum(added) + sum(deleted) as lines_touched, name from commits, commit_files
where commits.id = commit_files.id
and commits.author_when >= '2016-01-01'
group by name
order by lines_touched desc
limit 20;

-- Finding commits that changed a specific file
select summary, author_name, author_when from commits, commit_files
where commits.id = commit_files.id
and commit_files.name = 'README.md'
and commits.author_when >= '2016-01-01'
order by author_when desc;

-- Active days
-- Let's see which days of this project were the most active.
select count(*), date(author_when) from commits 
group by date(author_when) order by count(*) desc limit 10;

-- Active authors
select count(*), author_name, date(max(author_when)) from commits 
group by author_name order by count(*) desc;

-- day activity
select author_name, summary from commits where date(author_when) = '2023-05-14';

-- File Commits by author
SELECT
    author_name,
    COUNT(*)
FROM
	commits, commit_files
WHERE commits.id = commit_files.id
GROUP BY
	author_name
order by count(author_name) desc;

-- Files modified by a single author
SELECT DISTINCT cf.name, c.author_name
FROM commit_files cf
INNER JOIN commits c ON cf.id = c.id
WHERE cf.name IN (
    SELECT cf.name
    FROM commit_files cf
    INNER JOIN commits c ON cf.id = c.id
    GROUP BY cf.name
    HAVING COUNT(DISTINCT c.author_name) = 1
);

-- Files modified by many authors
SELECT DISTINCT cf.name, c.author_name
FROM commit_files cf
INNER JOIN commits c ON cf.id = c.id
WHERE cf.name IN (
    SELECT cf.name
    FROM commit_files cf
    INNER JOIN commits c ON cf.id = c.id
    GROUP BY cf.name
    HAVING COUNT(DISTINCT c.author_name) > 5
);



-- Authorship of a file
-- Let's look at who "knows" about a file based upon how much they've authored it. 
select name, author_name, count(commits.id) as commits, date(max(author_when)) as last_touched
from commits, commit_files 
where commits.id = commit_files.id
group by name, author_name order by count(commits.id) desc;

-- Authorship by weight
-- We can also look to see how many lines of code were added by someone, 
-- to go a little deeper. Maybe they only made 1 commit, but it changed every last thing?
select author_name, date(max(author_when)) as last_touched, sum(added) as added
from commits, commit_files where commits.id = commit_files.id and name = 'README.md'
group by author_name order by added desc;

-- Changes to a file
select summary, author_name, author_when from commits, commit_files 
where commits.id = commit_files.id and name = 'README.md' order by author_when desc;
