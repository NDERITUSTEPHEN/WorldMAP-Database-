from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Tuple

import pandas as pd
from rapidfuzz import fuzz

ALLOWED_TITLES = {
    "PASTOR",
    "BISHOP",
    "EVANGELIST",
    "BIBLE SCHOOL OVERSEER",
    "BIBLE OVERSEER",
    "BIBLE SCHOOL SUPERVISOR",
}

ALLOWED_LANGUAGES = {"KISWAHILI", "ENGLISH", "FRENCH"}

COUNTRY_CODE_MAP = {
    "KENYA": "+254",
    "KE": "+254",
    "TANZANIA": "+255",
    "TZ": "+255",
    "UNITED REPUBLIC OF TANZANIA": "+255",
}

REQUIRED_COLUMNS = [
    "Name",
    "Phone",
    "National ID",
    "Country",
    "Church Name",
    "Title",
    "Congregation Size",
    "Requested Language",
    "Have you received before?",
    "If yes, reason",
]

def norm_text(x) -> str:
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).strip())

def norm_country(x) -> str:
    return norm_text(x).upper()

def norm_title(x) -> str:
    s = norm_text(x).upper()
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s)
    return s

def norm_language(x) -> str:
    return norm_text(x).upper()

def norm_id(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"[\s\-]+", "", s)
    return s

def norm_name(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def split_name(full_name_norm: str) -> Tuple[str, str, str]:
    parts = [p for p in full_name_norm.split(" ") if p]
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return parts[0], " ".join(parts[1:-1]), parts[-1]

def name_key(full_name_norm: str) -> str:
    fn, mn, ln = split_name(full_name_norm)
    mid_initials = "".join([p[0] for p in mn.split() if p])
    return f"{fn[:1]}|{mid_initials}|{ln}"

def norm_phone(phone, country) -> str:
    if pd.isna(phone):
        return ""
    raw = re.sub(r"[^\d+]", "", str(phone).strip())
    c = COUNTRY_CODE_MAP.get(norm_country(country), "")
    if raw.startswith("+") and len(raw) >= 10:
        return raw
    if raw.startswith("254"):
        return "+254" + raw[3:]
    if raw.startswith("255"):
        return "+255" + raw[3:]
    if raw.startswith("0") and c:
        return c + raw[1:]
    return raw

def validate_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Required headers: {REQUIRED_COLUMNS}")

def prepare_applications(df: pd.DataFrame) -> pd.DataFrame:
    validate_columns(df)
    out = df.copy()

    out["full_name_original"] = out["Name"].astype(str)
    out["phone_original"] = out["Phone"].astype(str)
    out["national_id_original"] = out["National ID"].astype(str)
    out["country"] = out["Country"].apply(norm_country)
    out["church_name"] = out["Church Name"].apply(norm_text)
    out["title"] = out["Title"].apply(norm_title)
    out["congregation_size"] = pd.to_numeric(out["Congregation Size"], errors="coerce").fillna(0).astype(int)
    out["requested_language"] = out["Requested Language"].apply(norm_language)
    out["received_before"] = out["Have you received before?"].apply(norm_text)
    out["received_before_reason"] = out["If yes, reason"].apply(norm_text)

    out["full_name_normalized"] = out["full_name_original"].apply(norm_name)
    out["name_key"] = out["full_name_normalized"].apply(name_key)
    out["phone_normalized"] = out.apply(lambda r: norm_phone(r["phone_original"], r["country"]), axis=1)
    out["national_id_normalized"] = out["national_id_original"].apply(norm_id)

    return out[[
        "full_name_original","phone_original","national_id_original","country","church_name","title",
        "congregation_size","requested_language","received_before","received_before_reason",
        "full_name_normalized","name_key","phone_normalized","national_id_normalized"
    ]]

@dataclass
class FlagResult:
    is_disqualified: int
    disqualify_reason: str
    needs_review: int
    system_flags: str

def eligibility_flags(row: pd.Series) -> FlagResult:
    flags = []
    disq = 0
    disq_reason = ""

    if int(row.get("congregation_size", 0)) < 15:
        disq = 1
        disq_reason = "Congregation size < 15"
        flags.append("CONGREGATION_LT_15")

    title = str(row.get("title","")).strip().upper()
    if title and title not in ALLOWED_TITLES:
        flags.append("TITLE_NOT_ALLOWED")

    lang = str(row.get("requested_language","")).strip().upper()
    if lang and lang not in ALLOWED_LANGUAGES:
        flags.append("LANGUAGE_NOT_ALLOWED")

    rb = str(row.get("received_before","")).strip().upper()
    if rb in {"YES", "Y", "TRUE", "1"}:
        flags.append("SELF_REPORTED_PRIOR_RECEIPT")

    needs_review = 1 if flags else 0
    return FlagResult(disq, disq_reason, needs_review, ";".join(flags))

def fuzzy_name_score(a: str, b: str) -> int:
    return int(fuzz.token_set_ratio(a, b))


def name_parts(full_name_normalized: str) -> tuple[str, str, str]:
    """Return (first, middle, last) from a normalized full name."""
    parts = [p for p in str(full_name_normalized or "").strip().split(" ") if p]
    if not parts:
        return ("", "", "")
    if len(parts) == 1:
        return (parts[0], "", "")
    if len(parts) == 2:
        return (parts[0], "", parts[1])
    return (parts[0], " ".join(parts[1:-1]), parts[-1])

def middle_match(m1: str, m2: str, flexible: bool = True) -> bool:
    m1 = str(m1 or "").strip()
    m2 = str(m2 or "").strip()
    if not m1 or not m2:
        return False
    if m1 == m2:
        return True
    if flexible:
        return m1[0] == m2[0]
    return False

def similarity_tier(score: int) -> str:
    if score >= 92:
        return "HIGH"
    if 88 <= score <= 91:
        return "MEDIUM"
    return "NONE"
