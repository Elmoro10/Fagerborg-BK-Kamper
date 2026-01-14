#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
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

USER_AGENT = "Mozilla/5.0 (compatible; FagerborgBK-FixturesBot/1.0; +https://github.com/)"
HEADERS = {"User-Agent": USER_AGENT}

TIMEOUT = 20

TERMINLISTE_URL = "https://www.fotball.no/fotballdata/lag/kamper/?fiksId={fiksid}"

# ---- helpers -----------------------------------------------------

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
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:10]

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
    Download logo to assets/logos and return repo-relative path "assets/logos/..png".
    If download fails, return empty string.
    """
    url = normalize_url(url)
    if not url:
        return ""

    # choose extension
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext not in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"]:
        ext = ".png"

    fname = f"{safe_slug(team_name)}-{sha1(url)}{ext}"
    out_path = os.path.join(ASSETS_LOGO_DIR, fname)
    rel_path = f"assets/logos/{fname}"

    if os.path.exists(out_path) and os.path.getsize(out_path) > 500:
        return rel_path

    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        if r.status_code != 200:
            return ""
        ctype = (r.headers.get("Content-Type") or "").lower()
        # if it's html, skip
        if "text/html" in ctype:
            return ""

        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)

        # sanity check
        if os.path.getsize(out_path) < 500:
            try:
                os.remove(out_path)
            except OSError:
                pass
            return ""

        return rel_path
    except Exception:
        return ""

def parse_kickoff_to_utc_iso(date_str: str, time_str: str) -> str:
    """
    Very simple parser: expects Norwegian date and time like "08.04.2026" and "19:15".
    Produces UTC ISO string. We assume Europe/Oslo local time and convert to UTC by approximating:
    - Since DST exists, perfect conversion requires tz database.
    - To avoid wrong times, we store as UTC *without* DST math (treat local as UTC) is also wrong.
    For compatibility with your earlier requirement: fotball.no often already provides ISO or a timestamp.
    If you already parse ISO Z from the page, use that instead and ignore this.
    """
    # If you already have an ISO string, return it
    if "T" in (date_str or "") and (date_str.endswith("Z") or "+" in date_str):
        return date_str

    # Fallback: assume date_str is dd.mm.yyyy and time_str is HH:MM
    try:
        d = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
        # Treat as UTC to avoid DST mistakes (least surprising for consumers who parse Z)
        return d.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""

# ---- scraping -----------------------------------------------------

def fetch_team_matches(fiksid: int, team_label: str) -> dict:
    """
    Returns:
      {
        "fiksId": int,
        "teamName": "Fagerborg",
        "tournament": "...",
        "matches": [...]
      }
    """
    url = TERMINLISTE_URL.format(fiksid=fiksid)
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # NOTE: fotball.no markup can change.
    # You likely already have a working parser; keep that and just keep logo handling below.

    # Attempt to detect tournament heading (best effort)
    tournament = ""
    h1 = soup.find(["h1","h2"])
    if h1:
        tournament = h1.get_text(" ", strip=True)

    matches = []
    # Generic heuristic: each match row often sits in a table
    table = soup.find("table")
    rows = table.find_all("tr") if table else []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        # Extremely heuristic mapping (you should align with your real parser)
        # Common pattern:
        # [date] [time] [match] [result] [venue] ...
        raw = [td.get_text(" ", strip=True) for td in tds]

        # Find match link if any
        a = tr.find("a", href=True)
        match_url = urljoin("https://www.fotball.no", a["href"]) if a else ""

        # Try to extract teams like "Home - Away"
        home_team = ""
        away_team = ""
        teams_txt = ""
        for s in raw:
            if " - " in s and len(s) < 80:
                teams_txt = s
                break
        if teams_txt:
            parts = [p.strip() for p in teams_txt.split(" - ", 1)]
            if len(parts) == 2:
                home_team, away_team = parts

        # Date/time best effort
        date_txt = raw[0] if raw else ""
        time_txt = ""
        # try find time
        for s in raw:
            if re.match(r"^\d{1,2}:\d{2}$", s):
                time_txt = s
                break

        kickoff = parse_kickoff_to_utc_iso(date_txt, time_txt)

        # result best effort "2 - 1"
        home_goals = None
        away_goals = None
        status = "SCHEDULED"
        result_txt = ""
        for s in raw:
            if re.search(r"\d+\s*[-–]\s*\d+", s):
                result_txt = s
                break
        if result_txt:
            m = re.search(r"(\d+)\s*[-–]\s*(\d+)", result_txt)
            if m:
                home_goals = int(m.group(1))
                away_goals = int(m.group(2))
                status = "FINISHED"

        # venue best effort
        venue = ""
        if len(raw) >= 5:
            venue = raw[-1]

        # ---- logo handling (critical fix) ----
        # Try to find images for teams if present
        # If not present, you can later swap to a known endpoint.
        imgs = tr.find_all("img")
        home_logo_remote = ""
        away_logo_remote = ""
        if imgs:
            # heuristics: first two images
            if len(imgs) >= 1 and imgs[0].get("src"):
                home_logo_remote = imgs[0]["src"]
            if len(imgs) >= 2 and imgs[1].get("src"):
                away_logo_remote = imgs[1]["src"]

        # Download locally (will solve iframe issues)
        home_logo_local = download_logo(home_logo_remote, home_team) if home_team else ""
        away_logo_local = download_logo(away_logo_remote, away_team) if away_team else ""

        matches.append({
            "matchId": sha1(match_url or (home_team + away_team + kickoff)),
            "kickoff": kickoff,
            "homeTeam": home_team,
            "awayTeam": away_team,
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
        "teamName": "Fagerborg",
        "tournament": tournament or "",
        "matches": matches
    }

# ---- ICS generation (simple + compatible UTC Z) -------------------

def ics_escape(s: str) -> str:
    s = (s or "")
    s = s.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    s = s.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
    return s

def iso_to_dt_utc(iso: str) -> datetime:
    # expects ...Z
    if not iso:
        return None
    try:
        if iso.endswith("Z"):
            return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        # fallback
        return datetime.fromisoformat(iso).astimezone(timezone.utc)
    except Exception:
        return None

def build_ics(team_key: str, team_obj: dict, out_path: str):
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Fagerborg BK//Terminliste//NO",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:Fagerborg BK " + ("A-lag" if team_key=="a" else "B-lag" if team_key=="b" else "Alle"),
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

def main():
    ensure_dirs()

    a = fetch_team_matches(A_FIKS_ID, "a")
    b = fetch_team_matches(B_FIKS_ID, "b")

    out = {
        "updatedAt": utc_now_str(),
        "a": a,
        "b": b
    }

    with open(os.path.join(DATA_DIR, "matches.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # ICS
    build_ics("a", a, os.path.join(DATA_DIR, "a.ics"))
    build_ics("b", b, os.path.join(DATA_DIR, "b.ics"))
    # all.ics = merge
    merged = {"matches": (a.get("matches", []) + b.get("matches", []))}
    build_ics("all", merged, os.path.join(DATA_DIR, "all.ics"))

if __name__ == "__main__":
    main()
