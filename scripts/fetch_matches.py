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

# --- IDs (these MUST be correct for your setup) ---
A_TEAM_FIKS_ID = 311
B_TEAM_FIKS_ID = 6458
A_TOURN_FIKS_ID = 205403
B_TOURN_FIKS_ID = 205410

A_BADGE = "5. divisjon (2026)"
B_BADGE = "7. divisjon (2026)"
SEASON_YEAR = 2026

USER_AGENT = "Mozilla/5.0 (compatible; FagerborgBK-FixturesBot/3.1; +https://github.com/)"
HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT = 25

TEAM_MATCHES_URL = "https://www.fotball.no/fotballdata/lag/kamper/?fiksId={fiksid}"
TOURN_MATCHES_URL = "https://www.fotball.no/fotballdata/turnering/kamper/?fiksId={fiksid}"

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


def read_json_if_exists(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def write_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def http_get(session: requests.Session, url: str, tries: int = 3) -> str:
    last_err = None
    for i in range(tries):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200 and r.text and len(r.text) > 2000:
                return r.text
            last_err = RuntimeError(f"HTTP {r.status_code} for {url}")
        except Exception as e:
            last_err = e
    raise last_err


def download_logo(session: requests.Session, url: str, team_name: str) -> str:
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
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
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


def extract_row_text(tr) -> str:
    return " ".join(tr.get_text(" ", strip=True).split())


def parse_kickoff_iso_from_row_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4}).*?(\d{1,2})[:.](\d{2})", t)
    if m:
        dd, mm, yyyy, hh, mi = m.groups()
        try:
            d = datetime(int(yyyy), int(mm), int(dd), int(hh), int(mi), tzinfo=timezone.utc)
            return d.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return ""
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", t)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            d = datetime(int(yyyy), int(mm), int(dd), 0, 0, tzinfo=timezone.utc)
            return d.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return ""
    return ""


def extract_teams_from_row(tr) -> tuple[str, str]:
    txt = extract_row_text(tr)
    for sep in [" - ", " – ", " — ", " − "]:
        if sep in txt:
            left, right = txt.split(sep, 1)
            left = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", "", left)
            left = re.sub(r"\b\d{1,2}[:.]\d{2}\b", "", left).strip()
            right = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", "", right)
            right = re.sub(r"\b\d{1,2}[:.]\d{2}\b", "", right).strip()
            if len(left) > 80:
                left = " ".join(left.split()[-4:])
            if len(right) > 80:
                right = " ".join(right.split()[:4])
            return left.strip(), right.strip()

    tds = tr.find_all("td")
    cand = []
    for td in tds:
        s = td.get_text(" ", strip=True)
        s = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", "", s)
        s = re.sub(r"\b\d{1,2}[:.]\d{2}\b", "", s)
        s = re.sub(r"\b\d+\s*[-–]\s*\d+\b", "", s).strip()
        if s and len(s) <= 45:
            cand.append(s)
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
    return urljoin("https://www.fotball.no", a["href"])


def extract_venue_from_row(tr) -> str:
    tds = tr.find_all("td")
    if not tds:
        return ""
    for td in reversed(tds):
        s = td.get_text(" ", strip=True).strip()
        if not s:
            continue
        if re.search(r"\b\d+\s*[-–]\s*\d+\b", s):
            continue
        if re.search(r"\d{1,2}[:.]\d{2}", s):
            continue
        if re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", s):
            continue
        if len(s) < 3:
            continue
        return s
    return ""


def extract_logo_urls_from_row(tr) -> tuple[str, str]:
    imgs = tr.find_all("img")
    urls = []
    for img in imgs:
        src = normalize_url(img.get("src") or "")
        if src:
            urls.append(src)
    return (urls[0] if len(urls) >= 1 else ""), (urls[1] if len(urls) >= 2 else "")


def parse_matches_from_html(session: requests.Session, html: str, require_year: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    rows = []
    for tbl in tables:
        tr_list = tbl.find_all("tr")
        hits = 0
        for tr in tr_list[:25]:
            if re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", extract_row_text(tr)):
                hits += 1
        if hits >= 2:
            rows = tr_list
            break
    if not rows:
        rows = soup.find_all("tr")

    out = []
    for tr in rows:
        row_txt = extract_row_text(tr)
        if not re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", row_txt):
            continue

        kickoff = parse_kickoff_iso_from_row_text(row_txt)
        if not kickoff or not kickoff.startswith(f"{require_year}-"):
            continue

        home, away = extract_teams_from_row(tr)
        if not home or not away:
            continue

        hg, ag = extract_result_from_row(tr)
        status = "SCHEDULED"
        if hg is not None and ag is not None:
            status = "FINISHED"
        if re.search(r"\bavlyst\b|\bcancel", row_txt, re.IGNORECASE):
            status = "CANCELLED"

        match_url = extract_match_url(tr)
        venue = extract_venue_from_row(tr)

        home_logo_remote, away_logo_remote = extract_logo_urls_from_row(tr)
        home_logo_local = download_logo(session, home_logo_remote, home) if home_logo_remote else ""
        away_logo_local = download_logo(session, away_logo_remote, away) if away_logo_remote else ""

        match_id = sha1(match_url or f"{kickoff}-{home}-{away}")

        out.append({
            "matchId": match_id,
            "kickoff": kickoff,
            "homeTeam": home,
            "awayTeam": away,
            "homeLogoUrl": home_logo_local,
            "awayLogoUrl": away_logo_local,
            "venue": venue,
            "status": status,
            "homeGoals": hg,
            "awayGoals": ag,
            "tournament": "",
            "round": "",
            "matchUrl": match_url
        })

    return out


def filter_only_fagerborg(matches: list[dict]) -> list[dict]:
    return [m for m in matches if FAGERBORG_RE.search(str(m.get("homeTeam",""))) or FAGERBORG_RE.search(str(m.get("awayTeam","")))]


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


def build_ics(team_key: str, matches: list[dict], out_path: str, cal_name: str):
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Fagerborg BK//Terminliste//NO",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(cal_name)}",
        "X-WR-TIMEZONE:UTC",
    ]

    for m in matches:
        dt = iso_to_dt_utc(m.get("kickoff", ""))
        if not dt:
            continue
        dtstart = dt.strftime("%Y%m%dT%H%M%SZ")
        dtend = (dt + timedelta(hours=2)).strftime("%Y%m%dT%H%M%SZ")

        uid = f"{m.get('matchId','')}-{team_key}@fagerborgbk"

        status = str(m.get("status", "SCHEDULED")).upper()
        ics_status = "CONFIRMED" if status != "CANCELLED" else "CANCELLED"

        home = str(m.get("homeTeam", "")).strip()
        away = str(m.get("awayTeam", "")).strip()
        summary = f"{home} – {away}"
        if m.get("homeGoals") is not None and m.get("awayGoals") is not None:
            summary += f" ({m['homeGoals']}-{m['awayGoals']})"

        desc_parts = []
        if m.get("tournament"):
            desc_parts.append(f"Turnering: {m['tournament']}")
        if m.get("round"):
            desc_parts.append(f"Runde: {m['round']}")
        if m.get("matchUrl"):
            desc_parts.append(f"URL: {m['matchUrl']}")
        desc = "\n".join(desc_parts)

        loc = str(m.get("venue", "")).strip()
        url = str(m.get("matchUrl", "")).strip()

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
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(tmp, out_path)


def main():
    ensure_dirs()

    matches_path = os.path.join(DATA_DIR, "matches.json")
    prev = read_json_if_exists(matches_path)

    session = requests.Session()

    try:
        # Fetch pages (with retries)
        a_team_html = http_get(session, TEAM_MATCHES_URL.format(fiksid=A_TEAM_FIKS_ID))
        b_team_html = http_get(session, TEAM_MATCHES_URL.format(fiksid=B_TEAM_FIKS_ID))
        a_div_html  = http_get(session, TOURN_MATCHES_URL.format(fiksid=A_TOURN_FIKS_ID))
        b_div_html  = http_get(session, TOURN_MATCHES_URL.format(fiksid=B_TOURN_FIKS_ID))

        # Parse
        a_team_all = parse_matches_from_html(session, a_team_html, SEASON_YEAR)
        b_team_all = parse_matches_from_html(session, b_team_html, SEASON_YEAR)
        a_div_all  = parse_matches_from_html(session, a_div_html,  SEASON_YEAR)
        b_div_all  = parse_matches_from_html(session, b_div_html,  SEASON_YEAR)

        a_matches = filter_only_fagerborg(a_team_all)
        b_matches = filter_only_fagerborg(b_team_all)

        # If parse yields nonsense (0 matches) but we have previous data -> keep previous
        if prev and (len(a_matches) == 0 or len(b_matches) == 0):
            print("WARNING: Parsed 0 Fagerborg matches for A or B. Keeping previous matches.json to avoid nuking.")
            return 0

        out = {
            "updatedAt": utc_now_str(),
            "a": {
                "fiksId": A_TEAM_FIKS_ID,
                "teamName": "Fagerborg",
                "badge": A_BADGE,
                "matches": a_matches,
                "allMatches": a_div_all,
            },
            "b": {
                "fiksId": B_TEAM_FIKS_ID,
                "teamName": "Fagerborg",
                "badge": B_BADGE,
                "matches": b_matches,
                "allMatches": b_div_all,
            }
        }

        write_json(matches_path, out)

        # ICS – only uses Fagerborg matches
        build_ics("a", a_matches, os.path.join(DATA_DIR, "a.ics"), "Fagerborg BK A-lag (2026)")
        build_ics("b", b_matches, os.path.join(DATA_DIR, "b.ics"), "Fagerborg BK B-lag (2026)")
        build_ics("all", (a_matches + b_matches), os.path.join(DATA_DIR, "all.ics"), "Fagerborg BK (A+B) (2026)")

        print(f"OK: A matches={len(a_matches)} | B matches={len(b_matches)} | A div={len(a_div_all)} | B div={len(b_div_all)}")
        return 0

    except Exception as e:
        # ✅ Never fail job. Keep previous data.
        print("ERROR: Scraper failed. Keeping previous files intact.")
        print(f"ERROR DETAILS: {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
