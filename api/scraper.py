"""
i3X LinkedIn Scraper
Run this script via cron to fetch LinkedIn posts with #i3X into the database.

Modes:
  A. Cookie-based (linkedin_api): Set LINKEDIN_LI_AT and LINKEDIN_JSESSIONID in .env
  B. OAuth: Set LINKEDIN_ACCESS_TOKEN in .env (obtain via the old /auth flow)

Cron example (every 6 hours):
  0 */6 * * * /path/to/venv/bin/python /path/to/api/scraper.py >> /var/log/i3x_scraper.log 2>&1
"""

import json
import logging
import os
import re
import sqlite3
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

try:
    from linkedin_api import Linkedin
    from requests.cookies import RequestsCookieJar
    HAS_LINKEDIN_API = True
except ImportError:
    HAS_LINKEDIN_API = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "feed.db"
ENV_PATH = Path(__file__).parent / ".env"
CESMII_ORG_URN = "urn:li:organization:17998123"
CESMII_COMPANY_URN = "17998123"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_env():
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

load_env()

LINKEDIN_EMAIL = os.environ.get("LINKEDIN_EMAIL", "")
LINKEDIN_PASSWORD = os.environ.get("LINKEDIN_PASSWORD", "")
LINKEDIN_LI_AT = os.environ.get("LINKEDIN_LI_AT", "")
LINKEDIN_JSESSIONID = os.environ.get("LINKEDIN_JSESSIONID", "")
LINKEDIN_ACCESS_TOKEN = os.environ.get("LINKEDIN_ACCESS_TOKEN", "")

USE_CREDENTIALS = HAS_LINKEDIN_API and (LINKEDIN_LI_AT or (LINKEDIN_EMAIL and LINKEDIN_PASSWORD))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            author TEXT,
            author_avatar TEXT,
            author_headline TEXT,
            text TEXT,
            image_url TEXT,
            post_age TEXT,
            is_cesmii_share INTEGER DEFAULT 0,
            cesmii_comment TEXT,
            posted_at TEXT,
            scraped_at TEXT
        );
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER,
            post_url TEXT,
            comment_author TEXT,
            comment_text TEXT,
            scraped_at TEXT,
            FOREIGN KEY (post_id) REFERENCES posts(id)
        );
        CREATE TABLE IF NOT EXISTS contributors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            urn TEXT UNIQUE,
            name TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            posts_found INTEGER DEFAULT 0,
            comments_found INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running'
        );
    """)
    conn.commit()
    conn.close()


def _insert_post(conn, post_data):
    """Insert a post into the database, returning 1 if new."""
    try:
        conn.execute(
            """INSERT OR IGNORE INTO posts
            (url, author, author_avatar, author_headline, text, image_url,
             post_age, is_cesmii_share, cesmii_comment, posted_at, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                post_data.get("url", ""),
                post_data.get("author", "LinkedIn User"),
                post_data.get("author_avatar", ""),
                post_data.get("author_headline", ""),
                post_data.get("text", ""),
                post_data.get("image_url", ""),
                post_data.get("post_age", "recently"),
                post_data.get("is_cesmii_share", 0),
                post_data.get("cesmii_comment", ""),
                post_data.get("posted_at"),
                datetime.now(timezone.utc).isoformat(),
            )
        )
        conn.commit()
        return 1
    except sqlite3.IntegrityError:
        return 0


def _save_contributor(conn, urn, name):
    if not urn:
        return
    try:
        conn.execute(
            "INSERT OR IGNORE INTO contributors (urn, name) VALUES (?, ?)",
            (urn, name)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass


def _start_scrape_log():
    conn = get_db()
    cursor = conn.execute("INSERT INTO scrape_log (status) VALUES ('running')")
    conn.commit()
    log_id = cursor.lastrowid
    conn.close()
    return log_id


def _finish_scrape_log(log_id, posts, comments, status):
    conn = get_db()
    conn.execute(
        """UPDATE scrape_log
           SET finished_at = CURRENT_TIMESTAMP,
               posts_found = ?, comments_found = ?, status = ?
           WHERE id = ?""",
        (posts, comments, status, log_id),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _calc_age(timestamp_ms):
    if not timestamp_ms:
        return "recently"
    created = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    diff = datetime.now(timezone.utc) - created
    if diff.days > 365:
        return f"{diff.days // 365}y"
    if diff.days > 30:
        return f"{diff.days // 30}mo"
    if diff.days > 0:
        return f"{diff.days}d"
    hours = diff.seconds // 3600
    if hours > 0:
        return f"{hours}h"
    return f"{diff.seconds // 60}m"


def _parse_age_to_timestamp(age_str, activity_id=None):
    if not age_str:
        return None
    age_str = age_str.strip().split()[0]
    match = re.match(r"^(\d+)\s*(mo|m|h|d|w|y)", age_str)
    if not match:
        return None
    val = int(match.group(1))
    unit = match.group(2)
    now = datetime.now(timezone.utc)
    if unit == "m":
        delta = timedelta(minutes=val)
    elif unit == "h":
        delta = timedelta(hours=val)
    elif unit == "d":
        delta = timedelta(days=val)
    elif unit == "w":
        delta = timedelta(weeks=val)
    elif unit == "mo":
        delta = timedelta(days=val * 30)
    elif unit == "y":
        delta = timedelta(days=val * 365)
    else:
        return None
    base = now - delta
    if activity_id:
        try:
            micro = int(str(activity_id)[-6:]) % 1000000
            base = base.replace(microsecond=micro)
        except (ValueError, TypeError):
            pass
    return base.isoformat()


# ---------------------------------------------------------------------------
# OAuth mode (LinkedIn REST API)
# ---------------------------------------------------------------------------

def _oauth_headers():
    return {
        "Authorization": f"Bearer {LINKEDIN_ACCESS_TOKEN}",
        "X-Restli-Protocol-Version": "2.0.0",
        "LinkedIn-Version": "202401",
    }


def _resolve_author(author_urn, headers):
    if not author_urn:
        return "LinkedIn User"
    try:
        if "organization" in author_urn:
            org_id = author_urn.split(":")[-1]
            resp = requests.get(
                f"https://api.linkedin.com/v2/organizations/{org_id}",
                headers=headers, timeout=10
            )
            if resp.status_code == 200:
                return resp.json().get("localizedName", "Organization")
        elif "person" in author_urn:
            resp = requests.get(
                "https://api.linkedin.com/v2/me",
                headers=headers, timeout=10
            )
            if resp.status_code == 200:
                d = resp.json()
                first = d.get("localizedFirstName", "")
                last = d.get("localizedLastName", "")
                return f"{first} {last}".strip() or "LinkedIn User"
    except requests.RequestException:
        pass
    return "LinkedIn User"


def _extract_image(share_content):
    media = share_content.get("media", [])
    if isinstance(media, list):
        for m in media:
            url = m.get("originalUrl") or (m.get("thumbnails", [{}])[0].get("url", "") if m.get("thumbnails") else "")
            if url:
                return url
    return ""


def _fetch_comments_oauth(conn, post_id_urn, post_url, headers):
    new_comments = 0
    try:
        resp = requests.get(
            f"https://api.linkedin.com/v2/socialActions/{post_id_urn}/comments",
            headers=headers,
            params={"count": 50},
            timeout=10
        )
        if resp.status_code != 200:
            return 0

        post_row = conn.execute("SELECT id FROM posts WHERE url = ?", (post_url,)).fetchone()
        post_db_id = post_row["id"] if post_row else None

        for c in resp.json().get("elements", []):
            comment_text = c.get("message", {}).get("text", "")
            if not comment_text.strip():
                continue
            actor_urn = c.get("actor", "")
            comment_author = _resolve_author(actor_urn, headers)
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO comments
                       (post_id, post_url, comment_author, comment_text, scraped_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (post_db_id, post_url, comment_author, comment_text.strip(),
                     datetime.now(timezone.utc).isoformat())
                )
                conn.commit()
                new_comments += 1
            except sqlite3.IntegrityError:
                pass
    except requests.RequestException:
        pass
    return new_comments


def _save_post_from_api(conn, element, headers, is_cesmii=False):
    commentary = element.get("commentary", "")
    if not commentary:
        return 0, 0

    post_id_urn = element.get("id", "")
    post_url = f"https://www.linkedin.com/feed/update/{post_id_urn}"
    author_urn = element.get("author", "")
    author_name = _resolve_author(author_urn, headers)

    image_url = ""
    content = element.get("content", {})
    if content:
        media = content.get("media", {})
        if media:
            image_url = media.get("id", "")

    created_at = element.get("createdAt", 0)
    post_age = _calc_age(created_at) if created_at else "recently"
    posted_at = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc).isoformat() if created_at else None

    cesmii_comment = ""
    is_cesmii_share = 1 if is_cesmii else 0
    reshare = element.get("reshareContext")
    if reshare and is_cesmii:
        cesmii_comment = commentary
        parent = reshare.get("parent", "")
        if parent:
            post_url = f"https://www.linkedin.com/feed/update/{parent}"

    post_data = {
        "url": post_url,
        "author": author_name,
        "text": commentary,
        "image_url": image_url,
        "post_age": post_age,
        "posted_at": posted_at,
        "is_cesmii_share": is_cesmii_share,
        "cesmii_comment": cesmii_comment,
    }

    saved = _insert_post(conn, post_data)

    new_comments = 0
    if saved:
        _save_contributor(conn, author_urn, author_name)
        new_comments = _fetch_comments_oauth(conn, post_id_urn, post_url, headers)

    return saved, new_comments


def _fetch_org_posts_oauth(headers, conn, org_urn):
    new_posts = 0
    new_comments = 0
    try:
        resp = requests.get(
            "https://api.linkedin.com/rest/posts",
            headers=headers,
            params={"author": org_urn, "q": "author", "count": 50},
            timeout=15
        )
        if resp.status_code == 200:
            for element in resp.json().get("elements", []):
                p, c = _save_post_from_api(conn, element, headers, is_cesmii=(org_urn == CESMII_ORG_URN))
                new_posts += p
                new_comments += c
    except requests.RequestException as e:
        logger.warning(f"Org posts fetch failed for {org_urn}: {e}")
    return new_posts, new_comments


def fetch_linkedin_posts():
    """Fetch posts via OAuth LinkedIn REST API."""
    if not LINKEDIN_ACCESS_TOKEN:
        logger.error("No LINKEDIN_ACCESS_TOKEN in .env. Cannot run OAuth scrape.")
        return

    log_id = _start_scrape_log()
    headers = _oauth_headers()
    conn = get_db()
    new_posts = 0
    new_comments = 0

    try:
        # Strategy 1: hashtag search via Posts API
        resp = requests.get(
            "https://api.linkedin.com/rest/posts",
            headers=headers,
            params={"q": "hashtag", "hashtag": "i3X", "count": 50},
            timeout=15
        )
        if resp.status_code == 200:
            for element in resp.json().get("elements", []):
                p, c = _save_post_from_api(conn, element, headers)
                new_posts += p
                new_comments += c
        else:
            logger.info(f"Posts API ({resp.status_code}): {resp.text[:200]}")

        # Strategy 2: CESMII org posts
        p, c = _fetch_org_posts_oauth(headers, conn, CESMII_ORG_URN)
        new_posts += p
        new_comments += c

        # Strategy 3: other admin orgs
        org_resp = requests.get(
            "https://api.linkedin.com/v2/organizationAcls",
            headers=headers,
            params={"q": "roleAssignee", "role": "ADMINISTRATOR", "count": 10},
            timeout=10
        )
        if org_resp.status_code == 200:
            for acl in org_resp.json().get("elements", []):
                org_urn = acl.get("organization", "")
                if org_urn and org_urn != CESMII_ORG_URN:
                    p, c = _fetch_org_posts_oauth(headers, conn, org_urn)
                    new_posts += p
                    new_comments += c

    except Exception as e:
        logger.error(f"OAuth scrape error: {e}")
        conn.close()
        _finish_scrape_log(log_id, new_posts, new_comments, f"failed: {e}")
        return

    conn.close()
    _finish_scrape_log(log_id, new_posts, new_comments, "completed")
    logger.info(f"OAuth scrape complete: {new_posts} new posts, {new_comments} new comments.")


# ---------------------------------------------------------------------------
# Credential mode (linkedin_api)
# ---------------------------------------------------------------------------

SEED_CONTRIBUTOR_URNS = [
    "ACoAAAFQV-4BDuuH2n1aYjB2vSOSYjg9sA_pjRw",  # Chris Misztur
]


def _get_linkedin_api():
    if LINKEDIN_LI_AT and LINKEDIN_JSESSIONID:
        jsessionid = LINKEDIN_JSESSIONID
        if not jsessionid.startswith('"'):
            jsessionid = f'"{jsessionid}"'
        jar = RequestsCookieJar()
        jar.set("li_at", LINKEDIN_LI_AT, domain=".linkedin.com", path="/")
        jar.set("JSESSIONID", jsessionid, domain=".linkedin.com", path="/")
        return Linkedin("", "", cookies=jar)
    return Linkedin(LINKEDIN_EMAIL, LINKEDIN_PASSWORD)


def _get_known_contributors():
    urns = list(SEED_CONTRIBUTOR_URNS)
    conn = get_db()
    rows = conn.execute("SELECT urn FROM contributors WHERE urn IS NOT NULL").fetchall()
    urns.extend(row["urn"] for row in rows)
    conn.close()
    return urns


def _safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        err = str(e)
        if "redirect" in err.lower() or "jsondecode" in err.lower():
            logger.warning(f"Session expired: {e}")
            return None
        logger.warning(f"API call failed: {e}")
        return None


def _cred_extract_activity_id(url_or_urn):
    match = re.search(r"activity[:/](\d+)", url_or_urn or "")
    return match.group(1) if match else ""


def _cred_extract_comment_author(comment):
    commenter = comment.get("commenterForDashConversion", {})
    attrs = commenter.get("image", {}).get("attributes", [])
    if attrs:
        mini = attrs[0].get("miniProfile", {})
        first = mini.get("firstName", "")
        last = mini.get("lastName", "")
        if first or last:
            return f"{first} {last}".strip()
    return "Unknown"


def _cred_extract_comment_text(comment):
    v2 = comment.get("commentV2", {})
    if v2 and v2.get("text"):
        return v2["text"]
    values = comment.get("comment", {}).get("values", [])
    if values:
        return values[0].get("value", "")
    return ""


def _cred_extract_profile_post_text(post):
    commentary = post.get("commentary", {})
    if isinstance(commentary, dict):
        text_obj = commentary.get("text", {})
        if isinstance(text_obj, dict):
            return text_obj.get("text", "")
    return ""


def _cred_extract_profile_post_author(post):
    actor = post.get("actor", {})
    if isinstance(actor, dict):
        name = actor.get("name", {})
        if isinstance(name, dict):
            return name.get("text", "Unknown")
    return "Unknown"


def _cred_extract_author_headline(post):
    actor = post.get("actor", {})
    if isinstance(actor, dict):
        desc = actor.get("description", {})
        if isinstance(desc, dict):
            return desc.get("text", "")
        subtext = actor.get("subDescription", {})
        if isinstance(subtext, dict):
            return subtext.get("text", "")
    return ""


def _cred_extract_avatar_url(post):
    actor = post.get("actor", {})
    if not isinstance(actor, dict):
        return ""
    img = actor.get("image", {})
    if not isinstance(img, dict):
        return ""
    attrs = img.get("attributes", [])
    if not attrs:
        return ""
    mini = attrs[0].get("miniProfile", {})
    pic = mini.get("picture", {}).get("com.linkedin.common.VectorImage", {})
    return _cred_best_image_url(pic)


def _cred_extract_post_image(post):
    content = post.get("content", {})
    if not isinstance(content, dict):
        return ""
    img_comp = content.get("com.linkedin.voyager.feed.render.ImageComponent", {})
    if img_comp:
        images = img_comp.get("images", [])
        if images:
            attrs = images[0].get("attributes", [])
            if attrs:
                vec = attrs[0].get("vectorImage", {})
                return _cred_best_image_url(vec)
    article = content.get("com.linkedin.voyager.feed.render.ArticleComponent", {})
    if article:
        lg = article.get("largeImage", {})
        if isinstance(lg, dict):
            attrs = lg.get("attributes", [])
            if attrs:
                vec = attrs[0].get("vectorImage", {})
                return _cred_best_image_url(vec)
    return ""


def _cred_extract_post_age(post):
    actor = post.get("actor", {})
    if isinstance(actor, dict):
        sub = actor.get("subDescription", {})
        if isinstance(sub, dict):
            return sub.get("text", "")
    return ""


def _cred_extract_post_timestamp(post):
    meta = post.get("updateMetadata", {})
    if isinstance(meta, dict):
        ts = meta.get("createdAt", 0)
        if ts:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return None


def _cred_best_image_url(vec):
    if not isinstance(vec, dict):
        return ""
    root = vec.get("rootUrl", "")
    artifacts = vec.get("artifacts", [])
    if not artifacts:
        return ""
    largest = max(artifacts, key=lambda a: a.get("width", 0))
    path = largest.get("fileIdentifyingUrlPathSegment", "")
    if root and path:
        return f"{root}{path}"
    if path and path.startswith("http"):
        return path
    return ""


def scrape_with_credentials():
    """Scrape LinkedIn using cookie credentials via linkedin_api."""
    log_id = _start_scrape_log()
    posts_found = 0
    comments_found = 0
    seen_urls = set()
    contributor_urns = set(_get_known_contributors())

    try:
        api = _get_linkedin_api()
        logger.info("Authenticated with LinkedIn (credential mode)")

        hashtag_posts = []

        # 1. Scan main feed
        logger.info("Fetching feed posts...")
        feed_posts = _safe_call(api.get_feed_posts, limit=50)
        if feed_posts:
            logger.info(f"Got {len(feed_posts)} feed posts")
            for post in feed_posts:
                content = (post.get("content") or "").lower()
                if "i3x" in content:
                    age = post.get("old", "")
                    post_url = post.get("url", "")
                    aid = _cred_extract_activity_id(post_url)
                    hashtag_posts.append({
                        "author": post.get("author_name", "Unknown"),
                        "author_avatar": "",
                        "author_headline": "",
                        "content": post.get("content", ""),
                        "image_url": "",
                        "post_age": age,
                        "posted_at": _parse_age_to_timestamp(age, aid),
                        "url": post_url,
                    })
                    profile_url = post.get("author_profile", "")
                    if profile_url:
                        contributor_urns.add(profile_url)

        # 2. Resolve feed contributor profile URLs to URN IDs
        for profile_url in list(contributor_urns):
            if not profile_url.startswith("http"):
                continue
            match = re.search(r"/in/([^/?]+)", profile_url)
            if not match:
                continue
            slug = match.group(1)
            logger.info(f"Looking up contributor: {slug}")
            people = _safe_call(api.search_people, keywords=slug, limit=3)
            if people:
                for p in people:
                    urn = p.get("urn_id", "")
                    if urn:
                        contributor_urns.add(urn)
            contributor_urns.discard(profile_url)

        # 3. Search for people related to i3X
        logger.info("Searching for i3X contributors...")
        people = _safe_call(api.search_people, keywords="i3X", limit=10)
        if people:
            for person in people:
                urn = person.get("urn_id", "")
                if urn:
                    contributor_urns.add(urn)

        # 4. Scan each contributor's profile posts
        for urn_id in contributor_urns:
            if urn_id.startswith("http"):
                continue
            logger.info(f"Fetching posts for URN: {urn_id}")
            profile_posts = _safe_call(api.get_profile_posts, urn_id=urn_id, post_count=30)
            if profile_posts is None:
                continue
            for post in profile_posts:
                text = _cred_extract_profile_post_text(post)
                if "i3x" in text.lower():
                    urn = post.get("updateMetadata", {}).get("urn", "")
                    aid = _cred_extract_activity_id(urn)
                    url = f"https://www.linkedin.com/feed/update/urn:li:activity:{aid}/" if aid else ""
                    author = _cred_extract_profile_post_author(post)
                    age = _cred_extract_post_age(post)
                    hashtag_posts.append({
                        "author": author,
                        "author_avatar": _cred_extract_avatar_url(post),
                        "author_headline": _cred_extract_author_headline(post),
                        "content": text,
                        "image_url": _cred_extract_post_image(post),
                        "post_age": age,
                        "posted_at": _cred_extract_post_timestamp(post) or _parse_age_to_timestamp(age, aid),
                        "url": url,
                    })
                    conn = get_db()
                    _save_contributor(conn, urn_id, author)
                    conn.close()

        # 5. Fetch CESMII company feed
        logger.info("Fetching CESMII company updates...")
        cesmii_updates = _safe_call(api.get_company_updates, urn_id=CESMII_COMPANY_URN, max_results=100)
        if cesmii_updates:
            for u in cesmii_updates:
                inner = u.get("value", {}).get("com.linkedin.voyager.feed.render.UpdateV2", {})
                if not inner:
                    continue
                s = json.dumps(inner, default=str)
                if "i3x" not in s.lower():
                    continue

                permalink = u.get("permalink", "")
                actor = inner.get("actor", {})
                actor_name = actor.get("name", {}).get("text", "Unknown") if isinstance(actor.get("name"), dict) else "Unknown"

                commentary = inner.get("commentary", {})
                text = ""
                if isinstance(commentary, dict):
                    t = commentary.get("text", {})
                    if isinstance(t, dict):
                        text = t.get("text", "")

                avatar = _cred_extract_avatar_url(inner)
                headline = _cred_extract_author_headline(inner)
                image = _cred_extract_post_image(inner)
                age = _cred_extract_post_age(inner)

                cesmii_comment = ""
                reshared = inner.get("resharedUpdate")
                if reshared:
                    cesmii_comment = text
                    orig_commentary = reshared.get("commentary", {})
                    text = ""
                    if isinstance(orig_commentary, dict):
                        t = orig_commentary.get("text", {})
                        if isinstance(t, dict):
                            text = t.get("text", "")
                    orig_actor = reshared.get("actor", {}).get("name", {}).get("text", actor_name)
                    actor_name = orig_actor
                    avatar = _cred_extract_avatar_url(reshared)
                    headline = _cred_extract_author_headline(reshared)
                    image = _cred_extract_post_image(reshared)

                hashtag_posts.append({
                    "author": actor_name,
                    "author_avatar": avatar,
                    "author_headline": headline,
                    "content": text,
                    "image_url": image,
                    "post_age": age,
                    "posted_at": _parse_age_to_timestamp(age, _cred_extract_activity_id(permalink)),
                    "url": permalink,
                    "is_cesmii_share": 1,
                    "cesmii_comment": cesmii_comment,
                })

            logger.info(f"Found {sum(1 for p in hashtag_posts if p.get('is_cesmii_share'))} CESMII posts/reshares")

        logger.info(f"Found {len(hashtag_posts)} total posts matching #i3X")

        # 6. Save posts and fetch comments
        conn = get_db()
        try:
            for post in hashtag_posts:
                url = post["url"]
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                post_data = {
                    "url": url,
                    "author": post["author"],
                    "author_avatar": post.get("author_avatar", ""),
                    "author_headline": post.get("author_headline", ""),
                    "text": post.get("content", ""),
                    "image_url": post.get("image_url", ""),
                    "post_age": post.get("post_age", ""),
                    "posted_at": post.get("posted_at"),
                    "is_cesmii_share": post.get("is_cesmii_share", 0),
                    "cesmii_comment": post.get("cesmii_comment", ""),
                }
                saved = _insert_post(conn, post_data)
                if not saved:
                    continue
                posts_found += 1

                activity_id = _cred_extract_activity_id(url)
                if not activity_id:
                    continue

                raw_comments = _safe_call(api.get_post_comments, activity_id, comment_count=100)
                if raw_comments is None:
                    logger.warning("Failed to fetch comments, continuing...")
                    continue

                post_row = conn.execute("SELECT id FROM posts WHERE url = ?", (url,)).fetchone()
                post_db_id = post_row["id"] if post_row else None

                for c in raw_comments:
                    if not c:
                        continue
                    c_author = _cred_extract_comment_author(c)
                    c_text = _cred_extract_comment_text(c)
                    if c_text.strip():
                        try:
                            conn.execute(
                                """INSERT OR IGNORE INTO comments
                                   (post_id, post_url, comment_author, comment_text, scraped_at)
                                   VALUES (?, ?, ?, ?, ?)""",
                                (post_db_id, url, c_author, c_text.strip(),
                                 datetime.now(timezone.utc).isoformat())
                            )
                            conn.commit()
                            comments_found += 1
                        except sqlite3.IntegrityError:
                            pass
        finally:
            conn.close()

        _finish_scrape_log(log_id, posts_found, comments_found, "completed")
        logger.info(f"Credential scrape complete: {posts_found} posts, {comments_found} comments")

    except Exception as e:
        logger.exception(f"Credential scrape failed: {e}")
        _finish_scrape_log(log_id, posts_found, comments_found, f"failed: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    if USE_CREDENTIALS:
        logger.info("Running credential-based scrape...")
        scrape_with_credentials()
    elif LINKEDIN_ACCESS_TOKEN:
        logger.info("Running OAuth-based scrape...")
        fetch_linkedin_posts()
    else:
        logger.error(
            "No LinkedIn credentials found. Set LINKEDIN_LI_AT + LINKEDIN_JSESSIONID "
            "(or LINKEDIN_ACCESS_TOKEN) in api/.env"
        )
        raise SystemExit(1)
