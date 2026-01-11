#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetcher terminliste fra fotball.no (turnering eller lag) for gitt fiksId,
skriver data/matches.json og genererer ICS:
- data/a.ics
- data/b.ics
- data/all.ics

Krav:
- requests + BeautifulSoup
- kickoff i ISO-8601 UTC ("Z")
- ingen stack traces i frontend (frontend håndterer dette)
- logoer: lastes ned til assets/logos/ for trygg hosting i GitHub Pages
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


BASE = "https://www.fotball.no"
OSLO = ZoneInfo("Europe/Oslo")

# Start med de fiksId-ene du ba om:
TEAM_CONFIG = {
    "a": {
        "fiksId": 205403,
        "teamName": "Fagerborg",
        "label": "A-laget",
    },
    "b": {
        "fiksId": 205410,
        "teamName": "Fagerborg",
        "label": "B-laget",
    },
}

OUT_JSON = "data/matches.json"
OUT_ICS_A = "data/a.ics"
OUT_ICS_B = "data/b.ics"
OUT_ICS_ALL = "data/all.ics"

LOGO_DIR = "assets/logos"
PLACEHOLDER_SVG = os.path.join(LOGO_DIR, "placeholder.svg")

UA = "Mozilla/5.0 (compatible; FagerborgBK-TermlisteBot/1.0; +https://github.com/)"
TIMEOUT = 25


@dataclass
class Match:
    matchId: str
    kickoff: str  # ISO-8601 UTC Z
    homeTeam: str
    awayTeam: str
    homeLogoUrl: str
    awayLogoUrl: str
    venue: str
    status: str  # SCHEDULED|FINISHED|CANCELLED|POSTPONED
    homeGoals: Optional[int]
    awayGoals: Optional[int]
    tournament: str
    round: str
    matchUrl: str


def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(LOGO_DIR, exist_ok=True)

    # Enkel placeholder (alltid tilgjengelig lokalt)
    if not os.path.exists(PLACEHOLDER_SVG):
        with open(PLACEHOLDER_SVG, "w", encoding="utf-8") as f:
            f.write(
                """<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#e5e7eb"/>
      <stop offset="1" stop-color="#f2f4f7"/>
    </linearGradient>
  </defs>
  <rect x="0" y="0" width="64" height="64" rx="32" fill="url(#g)"/>
  <path d="M32 16c8.84 0 16 7.16 16 16s-7.16 16-16 16S16 40.84 16 32s7.16-16 16-16z"
        fill="#ffffff" opacity="0.7"/>
  <path d="M22 36c3.2-2.8 6.4-4.2 10-4.2S38.8 33.2 42 36"
        stroke="#98a2b3" stroke-width="3" fill="none" stroke-linecap="round"/>
  <circle cx="26" cy="28" r="2.2" fill="#667085"/>
  <circle cx="38" cy="28" r="2.2" fill="#667085"/>
</svg>"""
            )


def http_get(url: str) -> requests.Response:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    resp = s.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp


def candidate_pages(fiks_id: int) -> List[str]:
    # Vi prøver turnering først (ofte brukt i tabellprosjekter), deretter lag.
    return [
        f"{BASE}/fotballdata/turnering/hjem/?fiksId={fiks_id}",
        f"{BASE}/fotballdata/lag/hjem/?fiksId={fiks_id}",
    ]


def find_matches_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """
    Finn tabellen som ser ut som kamptabellen (har Hjemmelag + Bortelag).
    """
    for table in soup.find_all("table"):
        ths = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if not ths:
            continue
        if "hjemmelag" in " ".join(ths) and "bortelag" in " ".join(ths):
            return table
    return None


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_time_cell(raw: str) -> Tuple[Optional[int], Optional[int], bool]:
    """
    Return (hour, minute, is_unknown).
    Fotball.no kan ha "02.59" i noen terminlister når tid ikke er satt.
    """
    raw = normalize_space(raw).replace(".", ":")
    m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
    if not m:
        return None, None, True
    hh = int(m.group(1))
    mm = int(m.group(2))
    # Heuristikk: 02:59 brukes ofte som placeholder
    if hh == 2 and mm == 59:
        return 0, 0, True
    return hh, mm, False


def parse_date_cell(raw: str) -> Optional[datetime]:
    raw = normalize_space(raw)
    # dd.mm.yyyy
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", raw)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return datetime(y, mo, d)


def to_utc_iso(dt_local: datetime) -> str:
    if dt_local.tzinfo is None:
        dt_local = dt_local.replace(tzinfo=OSLO)
    dt_utc = dt_local.astimezone(timezone.utc)
    return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def extract_match_url(row: BeautifulSoup) -> Optional[str]:
    a = row.find("a", href=True)
    if not a:
        return None
    href = a["href"]
    # typisk: /fotballdata/kamp/?fiksId=8975343
    full = urljoin(BASE, href)
    if "/fotballdata/kamp/" in full and "fiksId=" in full:
        return full
    return None


def match_id_from_url(match_url: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(match_url).query)
        fid = q.get("fiksId", [None])[0]
        if fid:
            return str(fid)
    except Exception:
        return None
    return None


def safe_filename_key(s: str) -> str:
    s = normalize_space(s).lower()
    s = s.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "team"


def download_logo(logo_url: str, team_name: str) -> str:
    """
    Laster ned logo til assets/logos og returnerer relativ URL som fungerer i GitHub Pages.
    Hvis noe feiler, returner placeholder.
    """
    if not logo_url:
        return "assets/logos/placeholder.svg"

    # Unngå å lagre samme logo flere ganger (hash på URL)
    h = hashlib.sha1(logo_url.encode("utf-8")).hexdigest()[:12]
    base_name = safe_filename_key(team_name)[:40] or "team"
    out_name = f"{base_name}-{h}"

    # Prøv å finne filendelse
    ext = ".png"
    path = urlparse(logo_url).path.lower()
    if path.endswith(".svg"):
        ext = ".svg"
    elif path.endswith(".jpg") or path.endswith(".jpeg"):
        ext = ".jpg"
    elif path.endswith(".webp"):
        ext = ".webp"
    elif path.endswith(".gif"):
        ext = ".gif"
    else:
        # fallback
        ext = ".png"

    out_rel = f"{LOGO_DIR}/{out_name}{ext}"
    out_fs = out_rel

    if os.path.exists(out_fs) and os.path.getsize(out_fs) > 200:
        return out_rel.replace("\\", "/")

    try:
        r = http_get(logo_url)
        content = r.content
        if not content or len(content) < 100:
            return "assets/logos/placeholder.svg"
        with open(out_fs, "wb") as f:
            f.write(content)
        return out_rel.replace("\\", "/")
    except Exception:
        return "assets/logos/placeholder.svg"


def parse_table_rows(table: BeautifulSoup) -> Tuple[List[Dict[str, str]], Dict[str, int]]:
    """
    Leser header->kolonneindex og returnerer rå rader som dict.
    """
    header_cells = [normalize_space(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    header_map: Dict[str, int] = {}
    for i, h in enumerate(header_cells):
        header_map[h.lower()] = i

    rows: List[Dict[str, str]] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        vals = [normalize_space(td.get_text(" ", strip=True)) for td in tds]
        rows.append({
            "_cells": vals,
        })
        # behold tr ved siden av via ._tr? Vi kan ikke serialisere bs4,
        # men vi henter url/logo direkte i egen loop med samme tr.
    return rows, header_map


def parse_matches_from_page(page_url: str, fiks_id: int) -> Tuple[str, List[Match]]:
    """
    Return (tournament_title, matches).
    """
    resp = http_get(page_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    h1 = soup.find("h1")
    page_title = normalize_space(h1.get_text(" ", strip=True)) if h1 else "2026"

    table = find_matches_table(soup)
    if not table:
        return page_title, []

    # bygg header mapping
    ths = [normalize_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
    idx = {name: i for i, name in enumerate(ths)}

    def col(*names: str) -> Optional[int]:
        for n in names:
            if n.lower() in idx:
                return idx[n.lower()]
        return None

    i_round = col("runde")
    i_date = col("dato")
    i_time = col("tid")
    i_home = col("hjemmelag")
    i_res = col("resultat", "resultat ")
    i_away = col("bortelag")
    i_venue = col("bane", "stadion")
    i_kampnr = col("kampnr.", "kampnr")

    matches: List[Match] = []
    seen = set()

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        cells_text = [normalize_space(td.get_text(" ", strip=True)) for td in tds]

        # Må ha minimum: dato + hjemmelag + bortelag
        if i_date is None or i_home is None or i_away is None:
            continue
        if i_date >= len(cells_text) or i_home >= len(cells_text) or i_away >= len(cells_text):
            continue

        date_raw = cells_text[i_date]
        home = cells_text[i_home]
        away = cells_text[i_away]

        if not date_raw or not home or not away:
            continue

        d = parse_date_cell(date_raw)
        if not d:
            continue

        time_raw = cells_text[i_time] if (i_time is not None and i_time < len(cells_text)) else ""
        hh, mm, unknown_time = parse_time_cell(time_raw)

        dt_local = datetime(d.year, d.month, d.day, hh or 0, mm or 0, tzinfo=OSLO)

        kickoff_iso = to_utc_iso(dt_local)

        # result
        res_raw = cells_text[i_res] if (i_res is not None and i_res < len(cells_text)) else "-"
        res_raw = normalize_space(res_raw)

        home_goals: Optional[int] = None
        away_goals: Optional[int] = None
        status = "SCHEDULED"

        mres = re.search(r"(\d+)\s*[-–]\s*(\d+)", res_raw)
        if mres:
            home_goals = int(mres.group(1))
            away_goals = int(mres.group(2))
            status = "FINISHED"
        else:
            # hvis fotball.no viser "Avlyst" etc
            low = res_raw.lower()
            if "avly" in low:
                status = "CANCELLED"
            elif "utsatt" in low or "omberam" in low:
                status = "POSTPONED"

        venue = cells_text[i_venue] if (i_venue is not None and i_venue < len(cells_text)) else ""
        venue = normalize_space(venue)

        rnd = ""
        if i_round is not None and i_round < len(cells_text):
            r = normalize_space(cells_text[i_round])
            if r and r.isdigit():
                rnd = f"Runde {r}"
            elif r:
                rnd = r
        if not rnd:
            rnd = ""

        # matchUrl og matchId: finn første kamp-lenke i raden
        match_url = None
        match_id = None

        # ofte er dato-cellen linket til kampdetaljer
        a = tr.find("a", href=True)
        if a:
            full = urljoin(BASE, a["href"])
            if "/fotballdata/kamp/" in full and "fiksId=" in full:
                match_url = full
                match_id = match_id_from_url(full)

        # fallback: hvis ikke kampurl finnes
        if not match_url:
            match_url = ""
        if not match_id:
            # stabil fallback id basert på (dato+lag)
            match_id = hashlib.sha1(f"{kickoff_iso}|{home}|{away}|{fiks_id}".encode("utf-8")).hexdigest()[:10]

        # logoer: prøv å hente img src i samme celler
        def logo_from_cell(col_idx: Optional[int]) -> str:
            if col_idx is None:
                return "assets/logos/placeholder.svg"
            if col_idx >= len(tds):
                return "assets/logos/placeholder.svg"
            img = tds[col_idx].find("img")
            if not img or not img.get("src"):
                return "assets/logos/placeholder.svg"
            src = img["src"].strip()
            if not src or src.endswith("/gfx/country.svg"):
                return "assets/logos/placeholder.svg"
            return urljoin(BASE, src)

        home_logo_src = logo_from_cell(i_home)
        away_logo_src = logo_from_cell(i_away)

        # last ned logo lokalt
        home_logo_local = download_logo(home_logo_src, home)
        away_logo_local = download_logo(away_logo_src, away)

        tournament = page_title  # ofte "… - 2026 - …"
        # gjør turnering litt renere for UI
        tournament = tournament.replace(" - Norges Fotballforbund", "").strip()

        key = (match_id, kickoff_iso, home, away)
        if key in seen:
            continue
        seen.add(key)

        matches.append(Match(
            matchId=str(match_id),
            kickoff=kickoff_iso,
            homeTeam=home,
            awayTeam=away,
            homeLogoUrl=home_logo_local.replace("\\", "/"),
            awayLogoUrl=away_logo_local.replace("\\", "/"),
            venue=venue,
            status=status,
            homeGoals=home_goals,
            awayGoals=away_goals,
            tournament=tournament,
            round=rnd,
            matchUrl=match_url,
        ))

    return page_title, matches


def fetch_team(team_key: str, cfg: Dict) -> Dict:
    fiks_id = int(cfg["fiksId"])
    team_name = cfg.get("teamName", "Fagerborg")

    last_title = "2026"
    matches: List[Match] = []

    for url in candidate_pages(fiks_id):
        try:
            title, ms = parse_matches_from_page(url, fiks_id)
            last_title = title or last_title
            # hvis vi får treff, bruk det og stopp
            if ms:
                matches = ms
                break
        except Exception:
            continue

    # sorteringsstabilitet i output (stigende)
    matches.sort(key=lambda m: m.kickoff)

    # Badge: prøv å plukke ut " - 2026 - " delen
    tournament_badge = "2026"
    m = re.search(r"^(.*?)\s*-\s*2026", last_title)
    if m:
        tournament_badge = m.group(1).strip()
    else:
        # fallback
        tournament_badge = "2026"

    return {
        "fiksId": fiks_id,
        "teamName": team_name,
        "tournament": f"{tournament_badge} (2026)" if tournament_badge != "2026" else "2026",
        "matches": [m.__dict__ for m in matches],
    }


def ics_escape(s: str) -> str:
    s = s or ""
    s = s.replace("\\", "\\\\")
    s = s.replace(";", r"\;")
    s = s.replace(",", r"\,")
    s = s.replace("\n", r"\n")
    return s


def ics_dt(dt_iso_z: str) -> datetime:
    # dt_iso_z: "2026-04-12T18:30:00Z"
    if not dt_iso_z:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(dt_iso_z.replace("Z", "+00:00")).astimezone(timezone.utc)


def match_status_to_ics(status: str) -> str:
    s = (status or "").upper()
    if s == "CANCELLED":
        return "CANCELLED"
    # POSTPONED finnes ikke alltid som ICS STATUS (RFC har TENTATIVE/CONFIRMED/CANCELLED),
    # så vi beholder CONFIRMED og legger hint i DESCRIPTION.
    return "CONFIRMED"


def build_ics(calendar_name: str, team_key: str, matches: List[Dict]) -> str:
    """
    Genererer ICS med UTC-tider (Z). Dette gir minst feil og best kompatibilitet.
    """
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    dtstamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Fagerborg BK//Terminliste 2026//NO",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(calendar_name)}",
        "X-WR-TIMEZONE:UTC",
    ]

    for m in matches:
        match_id = str(m.get("matchId") or "")
        uid = f"{match_id}-{team_key}@fagerborgbk"
        kickoff_iso = m.get("kickoff") or ""
        dtstart = ics_dt(kickoff_iso)
        dtend = dtstart + timedelta(hours=2)

        dtstart_s = dtstart.strftime("%Y%m%dT%H%M%SZ")
        dtend_s = dtend.strftime("%Y%m%dT%H%M%SZ")

        home = m.get("homeTeam") or "Hjemmelag"
        away = m.get("awayTeam") or "Bortelag"

        hg = m.get("homeGoals")
        ag = m.get("awayGoals")
        played = isinstance(hg, int) and isinstance(ag, int)

        summary = f"{home} – {away}"
        if played:
            summary = f"{summary} ({hg}–{ag})"

        tournament = m.get("tournament") or ""
        rnd = m.get("round") or ""
        url = m.get("matchUrl") or ""
        venue = m.get("venue") or ""

        desc_parts = []
        if tournament:
            desc_parts.append(f"Turnering: {tournament}")
        if rnd:
            desc_parts.append(f"Runde: {rnd}")
        if url:
            desc_parts.append(f"Kamp: {url}")
        description = "\n".join(desc_parts)

        ics_status = match_status_to_ics(m.get("status") or "")

        if (m.get("status") or "").upper() == "POSTPONED":
            if description:
                description += "\n"
            description += "Status: Utsatt/omberammet (detaljer på lenken over)."

        lines.extend([
            "BEGIN:VEVENT",
            f"UID:{ics_escape(uid)}",
            f"DTSTAMP:{dtstamp}",
            f"DTSTART:{dtstart_s}",
            f"DTEND:{dtend_s}",
            f"SUMMARY:{ics_escape(summary)}",
            f"DESCRIPTION:{ics_escape(description)}",
            f"STATUS:{ics_status}",
        ])

        if venue:
            lines.append(f"LOCATION:{ics_escape(venue)}")
        if url:
            lines.append(f"URL:{ics_escape(url)}")

        lines.append("END:VEVENT")

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


def write_file(path: str, content: str, binary: bool = False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if binary else "w"
    with open(path, mode) as f:
        if binary:
            f.write(content)  # type: ignore
        else:
            f.write(content)


def main():
    ensure_dirs()

    out: Dict = {
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

    a_data = fetch_team("a", TEAM_CONFIG["a"])
    b_data = fetch_team("b", TEAM_CONFIG["b"])

    out["a"] = a_data
    out["b"] = b_data

    # JSON
    write_file(OUT_JSON, json.dumps(out, ensure_ascii=False, indent=2))

    # ICS
    a_matches = a_data.get("matches", [])
    b_matches = b_data.get("matches", [])
    all_matches = sorted(a_matches + b_matches, key=lambda m: (m.get("kickoff") or ""))

    ics_a = build_ics("Fagerborg BK – A-laget (2026)", "a", a_matches)
    ics_b = build_ics("Fagerborg BK – B-laget (2026)", "b", b_matches)
    ics_all = build_ics("Fagerborg BK – Alle kamper (2026)", "all", all_matches)

    write_file(OUT_ICS_A, ics_a)
    write_file(OUT_ICS_B, ics_b)
    write_file(OUT_ICS_ALL, ics_all)

    print(f"Wrote: {OUT_JSON}, {OUT_ICS_A}, {OUT_ICS_B}, {OUT_ICS_ALL}")
    print(f"A matches: {len(a_matches)} | B matches: {len(b_matches)} | All: {len(all_matches)}")


if __name__ == "__main__":
    main()
