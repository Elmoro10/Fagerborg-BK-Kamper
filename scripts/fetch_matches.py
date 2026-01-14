#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data")
ASSETS_LOGO_DIR = os.path.join(ROOT, "assets", "logos")

A_FIKS_ID = 205403
B_FIKS_ID = 205410

USER_AGENT = "Mozilla/5.0 (compatible; FagerborgBK-FixturesBot/2.0; +https://github.com/)"
HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT = 25

# Stable public page for schedule
TERMINLISTE_URL = "https://www.fotball.no/fotballdata/lag/kamper/?fiksId={fiksid}"

FAGERBORG_RE = re.compile(r"\bfagerborg\b", re.IGNORECASE)


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ASSETS_LOGO_DIR, exist_ok=True)


def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def safe_slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "team"


def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:12]


def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http://"):
        return "https://" + u[len("http://"):]
    return u


def download_logo(url: str, team_name: str) -> str:
    """
    Downloads logo to assets/logos and returns repo-relative path "assets/logos/..ext".
    If download fails, returns "" (frontend uses fallback circle).
    """
    url = normalize_url(url)
    if not url:
        return ""

    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext not in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"]:
        ext = ".png"

    fname = f"{safe_slug(team_name)}-{sha1(url)}{ext}"
    out_path = os.path.join(ASSETS_LOGO_DIR, fname)
    rel_path = f"assets/logos/{fname}"

    if os.path.exists(out_path) and os.path.getsize(out_path) > 600:
        return rel_path

    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        if r.status_code != 200:
            return ""
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype:
            return ""

        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)

        if os.path.getsize(out_path) < 600:
            try:
                os.remove(out_path)
            except OSError:
                pass
            return ""

        return rel_path
    except Exception:
        return ""


def parse_kickoff_iso_from_row_text(text: str) -> str:
    """
    Tries multiple formats and returns UTC ISO 'YYYY-MM-DDTHH:MM:SSZ'.
    We keep it simple and store as UTC to avoid DST mistakes.
    """
    t = (text or "").strip()
    if not t:
        return ""

    # already ISO?
    if "T" in t and (t.endswith("Z") or "+" in t):
        return t

    # dd.mm.yyyy HH:MM
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4}).*?(\d{1,2}):(\d{2})", t)
    if m:
        dd, mm, yyyy, hh, mi = m.groups()
        try:
            d = datetime(int(yyyy), int(mm), int(dd), int(hh), int(mi), tzinfo=timezone.utc)
            return d.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return ""

    # dd.mm.yyyy (no time) -> 00:00
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", t)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            d = datetime(int(yyyy), int(mm), int(dd), 0, 0, tzinfo=timezone.utc)
            return d.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return ""

    return ""


def extract_row_text(tr) -> str:
    return " ".join(tr.get_text(" ", strip=True).split())


def extract_teams_from_row(tr) -> tuple[str, str]:
    """
    Best effort team parsing:
    - if we find 'home - away' like pattern
    - or separate columns with team names
    """
    txt = extract_row_text(tr)

    # common separators: " - " or " – " or " — "
    for sep in [" - ", " – ", " — ", " − "]:
        if sep in txt:
            parts = txt.split(sep, 1)
            left = parts[0].strip()
            right = parts[1].strip()
            # left might contain date/time before team names; try to get last word-group
            # We'll find last 1–4 words that look like a name by removing date patterns:
            left_clean = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", "", left)
            left_clean = re.sub(r"\b\d{1,2}:\d{2}\b", "", left_clean).strip()
            right_clean = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", "", right)
            right_clean = re.sub(r"\b\d{1,2}:\d{2}\b", "", right_clean).strip()

            # if still huge, keep last chunk
            if len(left_clean) > 60:
                left_clean = left_clean.split()[-4:]
                left_clean = " ".join(left_clean)
            if len(right_clean) > 60:
                right_clean = right_clean.split()[:4]
                right_clean = " ".join(right_clean)

            return left_clean.strip(), right_clean.strip()

    # fallback: try tds
    tds = tr.find_all("td")
    cand = []
    for td in tds:
        s = td.get_text(" ", strip=True)
        s = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", "", s)
        s = re.sub(r"\b\d{1,2}:\d{2}\b", "", s)
        s = re.sub(r"\b\d+\s*[-–]\s*\d+\b", "", s)
        s = s.strip()
        if s and len(s) <= 40:
            cand.append(s)

    # pick first two plausible names
    if len(cand) >= 2:
        return cand[0], cand[1]

    return "", ""


def extract_result_from_row(tr) -> tuple[int | None, int | None]:
    txt = extract_row_text(tr).replace("–", "-").replace("−", "-")
    m = re.search(r"\b(\d+)\s*-\s*(\d+)\b", txt)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def extract_match_url(tr) -> str:
    a = tr.find("a", href=True)
    if not a:
        return ""
    href = a["href"]
    return urljoin("https://www.fotball.no", href)


def extract_venue_from_row(tr) -> str:
    tds = tr.find_all("td")
    if not tds:
        return ""
    # venue is often last column; avoid grabbing result column by filtering patterns
    for td in reversed(tds):
        s = td.get_text(" ", strip=True).strip()
        if not s:
            continue
        if re.search(r"\b\d+\s*[-–]\s*\d+\b", s):
            continue
        if re.search(r"\d{1,2}:\d{2}", s):
            continue
        if re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", s):
            continue
        # avoid very short tokens
        if len(s) < 3:
            continue
        return s
    return ""


def extract_kickoff_from_row(tr) -> str:
    # best: find date+time anywhere in row text
    txt = extract_row_text(tr)
    iso = parse_kickoff_iso_from_row_text(txt)
    return iso


def extract_logo_urls_from_row(tr) -> tuple[str, str]:
    imgs = tr.find_all("img")
    urls = []
    for img in imgs:
        src = img.get("src") or ""
        src = normalize_url(src)
        if src:
            urls.append(src)
    # heuristic: first is home, second is away
    home = urls[0] if len(urls) >= 1 else ""
    away = urls[1] if len(urls) >= 2 else ""
    return home, away


def fetch_team_matches(fiksid: int, team_name: str) -> dict:
    url = TERMINLISTE_URL.format(fiksid=fiksid)
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Tournament best effort: use page title / h1, but we also guard below with validation.
    tournament = ""
    h1 = soup.find("h1")
    if h1:
        tournament = h1.get_text(" ", strip=True)

    matches = []
    # Pick the table that contains lots of rows with dates (most stable heuristic)
    tables = soup.find_all("table")
    candidate_rows = []
    for tbl in tables:
      rows = tbl.find_all("tr")
      # look for at least a few rows containing a date pattern
      date_hits = 0
      for tr in rows[:20]:
          if re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", extract_row_text(tr)):
              date_hits += 1
      if date_hits >= 2:
          candidate_rows = rows
          break

    if not candidate_rows:
        # fallback: all rows
        candidate_rows = soup.find_all("tr")

    for tr in candidate_rows:
        row_txt = extract_row_text(tr)
        if not re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", row_txt):
            continue

        kickoff = extract_kickoff_from_row(tr)
        home, away = extract_teams_from_row(tr)
        if not kickoff or not home or not away:
            # keep going; we don't want garbage
            continue

        home_goals, away_goals = extract_result_from_row(tr)
        status = "SCHEDULED"
        if home_goals is not None and away_goals is not None:
            status = "FINISHED"
        if re.search(r"\bavlyst\b|\bcancel", row_txt, re.IGNORECASE):
            status = "CANCELLED"

        match_url = extract_match_url(tr)
        venue = extract_venue_from_row(tr)

        home_logo_remote, away_logo_remote = extract_logo_urls_from_row(tr)
        home_logo_local = download_logo(home_logo_remote, home) if home_logo_remote else ""
        away_logo_local = download_logo(away_logo_remote, away) if away_logo_remote else ""

        match_id = sha1(match_url or f"{fiksid}-{kickoff}-{home}-{away}")

        matches.append({
            "matchId": match_id,
            "kickoff": kickoff,
            "homeTeam": home,
            "awayTeam": away,
            "homeLogoUrl": home_logo_local,
            "awayLogoUrl": away_logo_local,
            "venue": venue,
            "status": status,
            "homeGoals": home_goals,
            "awayGoals": away_goals,
            "tournament": tournament or "",
            "round": "",
            "matchUrl": match_url
        })

    return {
        "fiksId": fiksid,
        "teamName": team_name,
        "tournament": tournament or "",
        "matches": matches
    }


def is_valid_fagerborg_dataset(team_obj: dict) -> bool:
    """
    Validation that prevents nuking the repo with wrong team content.
    We require at least 1 match where home or away contains 'Fagerborg'.
    """
    ms = (team_obj or {}).get("matches") or []
    for m in ms:
        home = str(m.get("homeTeam",""))
        away = str(m.get("awayTeam",""))
        if FAGERBORG_RE.search(home) or FAGERBORG_RE.search(away):
            return True
    return False


# ---- ICS generation (UTC Z for compatibility) ---------------------

def ics_escape(s: str) -> str:
    s = (s or "")
    s = s.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    s = s.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
    return s


def iso_to_dt_utc(iso: str):
    if not iso:
        return None
    try:
        if iso.endswith("Z"):
            return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(iso).astimezone(timezone.utc)
    except Exception:
        return None


def build_ics(team_key: str, team_obj: dict, out_path: str):
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cal_name = "Fagerborg BK"
    if team_key == "a":
        cal_name += " A-lag"
    elif team_key == "b":
        cal_name += " B-lag"
    else:
        cal_name += " Alle"

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Fagerborg BK//Terminliste//NO",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(cal_name)}",
        "X-WR-TIMEZONE:UTC",
    ]

    matches = (team_obj or {}).get("matches") or []
    for m in matches:
        dt = iso_to_dt_utc(m.get("kickoff",""))
        if not dt:
            continue

        dtstart = dt.strftime("%Y%m%dT%H%M%SZ")
        dtend = (dt + timedelta(hours=2)).strftime("%Y%m%dT%H%M%SZ")

        mid = str(m.get("matchId",""))
        uid = f"{mid}-{team_key}@fagerborgbk"

        status = str(m.get("status","SCHEDULED")).upper()
        ics_status = "CONFIRMED"
        if status == "CANCELLED":
            ics_status = "CANCELLED"

        home = str(m.get("homeTeam","")).strip()
        away = str(m.get("awayTeam","")).strip()
        summary = f"{home} – {away}"
        if m.get("homeGoals") is not None and m.get("awayGoals") is not None:
            summary += f" ({m['homeGoals']}-{m['awayGoals']})"

        desc_parts = []
        if m.get("tournament"): desc_parts.append(f"Turnering: {m['tournament']}")
        if m.get("round"): desc_parts.append(f"Runde: {m['round']}")
        if m.get("matchUrl"): desc_parts.append(f"URL: {m['matchUrl']}")
        desc = "\n".join(desc_parts)

        loc = str(m.get("venue","")).strip()
        url = str(m.get("matchUrl","")).strip()

        lines += [
            "BEGIN:VEVENT",
            f"UID:{ics_escape(uid)}",
            f"DTSTAMP:{now}",
            f"DTSTART:{dtstart}",
            f"DTEND:{dtend}",
            f"SUMMARY:{ics_escape(summary)}",
            f"DESCRIPTION:{ics_escape(desc)}",
            f"LOCATION:{ics_escape(loc)}" if loc else "LOCATION:",
            f"URL:{ics_escape(url)}" if url else "URL:",
            f"STATUS:{ics_status}",
            "END:VEVENT",
        ]

    lines.append("END:VCALENDAR")
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


def load_previous():
    prev_path = os.path.join(DATA_DIR, "matches.json")
    if not os.path.exists(prev_path):
        return None
    try:
        with open(prev_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def main():
    ensure_dirs()
    prev = load_previous()

    # Try fetch fresh
    a_new = fetch_team_matches(A_FIKS_ID, "Fagerborg")
    b_new = fetch_team_matches(B_FIKS_ID, "Fagerborg")

    a_ok = is_valid_fagerborg_dataset(a_new)
    b_ok = is_valid_fagerborg_dataset(b_new)

    if not a_ok or not b_ok:
        # Don't overwrite working data if we got wrong page or empty parse.
        # Keep old data if available.
        if prev and isinstance(prev, dict):
            print("VALIDATION FAILED. Keeping previous data/matches.json to avoid nuking UI.")
            if not a_ok:
                print("A dataset invalid (no Fagerborg matches parsed).")
            if not b_ok:
                print("B dataset invalid (no Fagerborg matches parsed).")

            # Still rebuild ICS from previous if present (optional; keeps consistency)
            try:
                a_prev = prev.get("a") or {}
                b_prev = prev.get("b") or {}
                build_ics("a", a_prev, os.path.join(DATA_DIR, "a.ics"))
                build_ics("b", b_prev, os.path.join(DATA_DIR, "b.ics"))
                merged = {"matches": (a_prev.get("matches", []) + b_prev.get("matches", []))}
                build_ics("all", merged, os.path.join(DATA_DIR, "all.ics"))
            except Exception:
                pass

            return

        # No previous data -> write minimal safe dataset (won't crash UI)
        print("VALIDATION FAILED and no previous data exists. Writing empty safe dataset.")
        out = {
            "updatedAt": utc_now_str(),
            "a": {"fiksId": A_FIKS_ID, "teamName":"Fagerborg", "tournament":"", "matches":[]},
            "b": {"fiksId": B_FIKS_ID, "teamName":"Fagerborg", "tournament":"", "matches":[]},
        }
        with open(os.path.join(DATA_DIR, "matches.json"), "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        build_ics("a", out["a"], os.path.join(DATA_DIR, "a.ics"))
        build_ics("b", out["b"], os.path.join(DATA_DIR, "b.ics"))
        build_ics("all", {"matches":[]}, os.path.join(DATA_DIR, "all.ics"))
        return

    # If valid -> write new dataset
    out = {
        "updatedAt": utc_now_str(),
        "a": a_new,
        "b": b_new
    }
    with open(os.path.join(DATA_DIR, "matches.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    build_ics("a", a_new, os.path.join(DATA_DIR, "a.ics"))
    build_ics("b", b_new, os.path.join(DATA_DIR, "b.ics"))
    merged = {"matches": (a_new.get("matches", []) + b_new.get("matches", []))}
    build_ics("all", merged, os.path.join(DATA_DIR, "all.ics"))


if __name__ == "__main__":
    main()
