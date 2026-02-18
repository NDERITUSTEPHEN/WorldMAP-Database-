import io
from datetime import date
import uuid

import pandas as pd
import streamlit as st


def safe_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Make dataframe display-safe for Streamlit (avoid Arrow conversion errors)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        try:
            if out[c].dtype == "object" and out[c].map(type).nunique() > 1:
                out[c] = out[c].astype(str)
        except Exception:
            try:
                out[c] = out[c].astype(str)
            except Exception:
                pass
    return out

from dedupe import prepare_applications, eligibility_flags, split_name, fuzzy_name_score, ALLOWED_TITLES
from db import (
    init_db,
    create_batch,
    fetch_batches,
    insert_applications,
    fetch_applications,
    fetch_persons,
    fetch_issuances,
    upsert_person_from_application,
    set_application_status,
    insert_issuance,
)

SCHEMA_SQL = open("schema.sql", "r", encoding="utf-8").read()

st.set_page_config(page_title="WorldMap – Shepherd Staff (Local)", layout="wide")

st.markdown(
    """
<style>
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
.wm-card {
  border: 1px solid rgba(49, 51, 63, 0.15);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(250, 250, 250, 0.6);
  margin-bottom: 10px;
}
.wm-small { font-size: 0.92rem; opacity: 0.9; }
.wm-pill {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 999px;
  font-size: 0.85rem;
  border: 1px solid rgba(49, 51, 63, 0.18);
  margin-right: 8px;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("WorldMap – Shepherd Staff Distribution")
st.caption("✅ Check first, then commit only the clean batch to the database (SQLite).")

# ---------------- SETTINGS ----------------
st.sidebar.header("Settings")
db_path = st.sidebar.text_input("SQLite DB file", value="worldmap.db")
name_threshold = st.sidebar.slider("Name similarity threshold", 70, 100, 88)
admin_user = st.sidebar.text_input("Admin name", value="admin")

if st.sidebar.button("Initialize DB"):
    init_db(db_path, SCHEMA_SQL)
    st.sidebar.success("Database ready.")

with st.expander("✅ Excel format (must match exactly)", expanded=False):
    st.markdown(
        """
**Required headers:**
- Name
- Phone
- National ID
- Country
- Church Name
- Title
- Congregation Size
- Requested Language
- Have you received before?
- If yes, reason
"""
    )

# ---------------- UTILS ----------------
def missing_fields_mask(raw_df: pd.DataFrame) -> pd.Series:
    # Stage 1 required fields
    required = ["Title","Requested Language","Congregation Size"]
    mask = pd.Series(False, index=raw_df.index)
    for c in required:
        if c not in raw_df.columns:
            mask |= True
        else:
            mask |= raw_df[c].isna() | (raw_df[c].astype(str).str.strip() == "")
    return mask

def compute_matches(apps_df: pd.DataFrame, persons_df: pd.DataFrame, issuances_df: pd.DataFrame, threshold: int) -> pd.DataFrame:
    out = apps_df.copy()

    phone_to_person = {}
    id_to_person = {}
    for _, p in persons_df.iterrows():
        ph = str(p.get("phone_normalized") or "").strip()
        if ph:
            phone_to_person[ph] = int(p["person_id"])
        nid = str(p.get("national_id_normalized") or "").strip()
        if nid:
            id_to_person[nid] = int(p["person_id"])

    latest_issue = {}
    if not issuances_df.empty:
        for _, r in issuances_df.sort_values("issued_at", ascending=False).iterrows():
            pid = int(r["person_id"])
            if pid not in latest_issue:
                latest_issue[pid] = str(r["issued_at"])

    out["DuplicatePhone"] = False
    out["DuplicateID"] = False
    out["MatchedPersonID"] = ""
    out["PriorIssuanceLatest"] = ""
    out["TopMatch1"] = ""
    out["TopMatch2"] = ""
    out["TopMatch3"] = ""

    persons_df = persons_df.copy()
    persons_df["full_name_normalized"] = persons_df.get("full_name_normalized", "").fillna("").astype(str)
    persons_df["country"] = persons_df.get("country", "").fillna("").astype(str)
    persons_df["_last"] = persons_df["full_name_normalized"].apply(lambda s: s.split(" ")[-1] if s.strip() else "")

    country_last_to_rows = {}
    for _, r in persons_df.iterrows():
        key = (r["country"].strip().upper(), r["_last"].strip().upper())
        country_last_to_rows.setdefault(key, []).append(r)

    for i, row in out.iterrows():
        ph = str(row.get("phone_normalized") or "").strip()
        nid = str(row.get("national_id_normalized") or "").strip()

        matched_pid = None
        if ph and ph in phone_to_person:
            matched_pid = phone_to_person[ph]
            out.at[i, "DuplicatePhone"] = True
        if nid and nid in id_to_person:
            matched_pid = matched_pid or id_to_person[nid]
            out.at[i, "DuplicateID"] = True

        if matched_pid:
            out.at[i, "MatchedPersonID"] = str(matched_pid)
            if matched_pid in latest_issue:
                out.at[i, "PriorIssuanceLatest"] = latest_issue[matched_pid]

        fulln = str(row.get("full_name_normalized") or "").strip()
        if not fulln:
            continue

        fn, mn, ln = split_name(fulln)
        ctry = str(row.get("country") or "").strip().upper()
        key = (ctry, ln.strip().upper())

        candidates = country_last_to_rows.get(key, [])
        if not candidates:
            candidates = [r for _, r in persons_df[persons_df["country"].str.upper() == ctry].head(800).iterrows()]

        scored = []
        for pr in candidates:
            pn = str(pr.get("full_name_normalized") or "").strip()
            if not pn:
                continue
            score = fuzzy_name_score(fulln, pn)
            if score >= threshold:
                scored.append((int(pr["person_id"]), score, pn, str(pr.get("phone_normalized","")), str(pr.get("national_id_normalized","")), str(pr.get("church_name",""))))

        scored.sort(key=lambda x: x[1], reverse=True)
        top = scored[:3]

        def fmt(t):
            pid, score, pn, ph2, nid2, ch = t
            return f"person_id={pid} score={score} name={pn} phone={ph2} id={nid2} church={ch}"

        if len(top) > 0:
            out.at[i, "TopMatch1"] = fmt(top[0])
        if len(top) > 1:
            out.at[i, "TopMatch2"] = fmt(top[1])
        if len(top) > 2:
            out.at[i, "TopMatch3"] = fmt(top[2])

    return out

def build_review_grouped(apps_with_matches: pd.DataFrame):
    roles = []
    rows = []
    gid = 0

    for _, a in apps_with_matches.iterrows():
        match_lines = [str(a.get("TopMatch1","")).strip(), str(a.get("TopMatch2","")).strip(), str(a.get("TopMatch3","")).strip()]
        match_lines = [m for m in match_lines if m]

        hard = bool(a.get("DuplicatePhone")) or bool(a.get("DuplicateID"))
        prior = bool(str(a.get("PriorIssuanceLatest","")).strip())
        fuzzy = len(match_lines) > 0
        flagged = hard or prior or fuzzy or int(a.get("is_disqualified",0)) == 1 or int(a.get("needs_review",0)) == 1 or int(a.get("missing_required_fields",0)) == 1

        if not flagged:
            continue

        gid += 1
        primary = a.to_dict()
        primary["GroupID"] = gid
        reasons = []
        if int(a.get("missing_required_fields",0)) == 1:
            reasons.append("Missing required fields")
        if int(a.get("congregation_size",0)) < 15:
            reasons.append("Congregation size < 15")
        if str(a.get("title","")).strip().upper() not in ALLOWED_TITLES:
            reasons.append("Title not eligible")
        if bool(a.get("DuplicatePhone")):
            reasons.append("Phone duplicate")
        if bool(a.get("DuplicateID")):
            reasons.append("ID duplicate")
        if fuzzy:
            reasons.append("Name similarity")
        if prior:
            reasons.append("Prior issuance found")
        if int(a.get("needs_review",0)) == 1 and not reasons:
            reasons.append("Needs review")

        primary["MatchReason"] = " | ".join(reasons)
        primary["RowRole"] = "PRIMARY"
        rows.append(primary)
        roles.append("PRIMARY")

        for m in match_lines:
            mr = {k: None for k in apps_with_matches.columns}
            mr["MatchReason"] = "Match candidate"
            mr["RowRole"] = "MATCH"
            mr["GroupID"] = gid
            mr["TopMatch1"] = m
            rows.append(mr)
            roles.append("MATCH")

        sep = {k: None for k in apps_with_matches.columns}
        rows.append(sep)
        roles.append("SEP")

    review_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    return review_df, roles


def auto_status(row: pd.Series) -> tuple[str, str]:
    # Stage 1: completeness
    missing = []
    title = str(row.get("title","")).strip()
    lang = str(row.get("requested_language","")).strip()
    size_raw = row.get("congregation_size", "")
    try:
        size = int(float(size_raw)) if str(size_raw).strip() != "" else None
    except Exception:
        size = None

    if not title:
        missing.append("Missing Title")
    if not lang:
        missing.append("Missing Book Language")
    if size is None:
        missing.append("Missing/Invalid Congregation Size")

    if missing:
        return ("NEEDS_FOLLOW_UP", " | ".join(missing))

    # Stage 1: auto reject
    if size < 15:
        return ("REJECTED", "Congregation < 15")
    if title.strip().upper() not in ALLOWED_TITLES:
        return ("REJECTED", "Title not eligible")

    # Stage 2: duplicates & similarity
    hard = bool(row.get("DuplicatePhone")) or bool(row.get("DuplicateID"))
    prior = bool(str(row.get("PriorIssuanceLatest","")).strip())

    from dedupe import name_parts, middle_match, similarity_tier
    fulln = str(row.get("full_name_normalized","")).strip()
    _, mn, ln = name_parts(fulln)
    ctry = str(row.get("country","")).strip().upper()

    import re as _re
    sim_flag_high = False
    sim_flag_medium = False

    for col in ["TopMatch1","TopMatch2","TopMatch3"]:
        s = str(row.get(col,""))
        m = _re.search(r"score=(\d+)", s)
        n = _re.search(r"name=(.*?)(?: phone=| id=| church=|$)", s)
        if not m or not n:
            continue
        score = int(m.group(1))
        tier = similarity_tier(score)
        if tier == "HIGH":
            sim_flag_high = True
        elif tier == "MEDIUM":
            cand_name = n.group(1).strip()
            _, cmn, cln = name_parts(cand_name)
            if cln and ln and cln.upper() == ln.upper():
                if ctry:
                    pass
                if middle_match(mn, cmn, flexible=True):
                    sim_flag_medium = True

    if hard or prior or sim_flag_high or sim_flag_medium:
        reasons = []
        if bool(row.get("DuplicatePhone")):
            reasons.append("Phone duplicate")
        if bool(row.get("DuplicateID")):
            reasons.append("ID duplicate")
        if prior:
            reasons.append("Prior issuance found")
        if sim_flag_high:
            reasons.append("Name similarity (HIGH)")
        elif sim_flag_medium:
            reasons.append("Name similarity (MEDIUM)")
        return ("NEEDS_REVIEW", " | ".join(reasons) if reasons else "Needs review")

    return ("APPROVED_READY", "Complete + eligible + no duplicate risk")


    suspicious = bool(row.get("DuplicatePhone")) or bool(row.get("DuplicateID")) or bool(str(row.get("PriorIssuanceLatest","")).strip())
    name_sim = any(str(row.get(x,"")).strip() for x in ["TopMatch1","TopMatch2","TopMatch3"])
    if suspicious or name_sim or int(row.get("needs_review",0)) == 1:
        return ("NEEDS_REVIEW", "Possible duplicate/prior issuance/name similarity/system flags")

    return ("APPROVED_READY", "Clean eligible")


def extract_person_ids_from_topmatches(df: pd.DataFrame) -> pd.Series:
    # Parses 'person_id=123' from TopMatch1/2/3 strings
    import re as _re
    ids = []
    for _, r in df.iterrows():
        found = set()
        for c in ["TopMatch1","TopMatch2","TopMatch3"]:
            s = str(r.get(c,""))
            m = _re.search(r"person_id=(\d+)", s)
            if m:
                found.add(int(m.group(1)))
        ids.append(sorted(found))
    return pd.Series(ids, index=df.index)


def export_workbook(checked: pd.DataFrame, review_df: pd.DataFrame, roles: list[str]) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        checked.to_excel(w, index=False, sheet_name="AllChecked")

        def sheet(df, name):
            if df.empty:
                pd.DataFrame({"info":[f"No rows for {name}"]}).to_excel(w, index=False, sheet_name=name[:31])
            else:
                df.to_excel(w, index=False, sheet_name=name[:31])

        sheet(checked[checked["AutoStatus"]=="REJECTED"], "AutoRejected")
        sheet(checked[checked["AutoStatus"]=="NEEDS_FOLLOW_UP"], "NeedsFollowUp")
        sheet(checked[checked["AutoStatus"]=="NEEDS_REVIEW"], "NeedsReview")
        sheet(checked[checked["AutoStatus"]=="APPROVED_READY"], "ApprovedReady")

        if not review_df.empty:
            review_df.to_excel(w, index=False, sheet_name="Review_Grouped")
            wb = w.book
            ws = w.sheets["Review_Grouped"]
            fmt_primary = wb.add_format({"bg_color": "#FFC7CE"})
            fmt_match = wb.add_format({"bg_color": "#FFEB9C"})
            for r, role in enumerate(roles):
                excel_row = r + 1
                if role == "PRIMARY":
                    ws.set_row(excel_row, None, fmt_primary)
                elif role == "MATCH":
                    ws.set_row(excel_row, None, fmt_match)

        # Corrections template for follow-up (admin fills and re-uploads as a new upload batch)
        follow = checked[checked["AutoStatus"]=="NEEDS_FOLLOW_UP"].copy()
        if not follow.empty:
            templ = pd.DataFrame({
                "RowID": follow["RowID"],
                "Name": follow["full_name_original"],
                "Phone": follow["phone_original"],
                "National ID": follow["national_id_original"],
                "Country": follow["country"],
                "Church Name": follow["church_name"],
                "Title": follow["title"],
                "Congregation Size": follow["congregation_size"],
                "Requested Language": follow["requested_language"],
                "Have you received before?": follow["received_before"],
                "If yes, reason": follow["received_before_reason"],
            })
        else:
            templ = pd.DataFrame({"RowID":[]})
        templ.to_excel(w, index=False, sheet_name="Corrections_Template")
    return out.getvalue()

# ---------------- DB Metrics (existing DB only) ----------------
init_db(db_path, SCHEMA_SQL)
apps_db = fetch_applications(db_path)
persons_db = fetch_persons(db_path)
iss_db = fetch_issuances(db_path)

m1, m2, m3, m4 = st.columns(4)
m1.metric("DB Applications", 0 if apps_db.empty else len(apps_db))
m2.metric("DB People", 0 if persons_db.empty else len(persons_db))
m3.metric("DB Issuances", 0 if iss_db.empty else len(iss_db))
m4.metric("DB APPROVED_READY", 0 if apps_db.empty else int((apps_db["status"]=="APPROVED_READY").sum()))

# ---------------- Tabs ----------------
tab1, tab2, tab3, tab4 = st.tabs(["1) Upload → Check → Export", "2) Second check + Commit clean batch", "3) Bulk issue from DB", "4) Reports & Batches"])

with tab1:
    st.subheader("Upload Excel files (NOT saved to DB yet)")
    st.markdown("<div class='wm-card'><span class='wm-pill'>Policy</span><span class='wm-small'>Uploads stay in memory only. We run all checks first. You export results, then you choose to commit only the clean rows.</span></div>", unsafe_allow_html=True)

    uploads = st.file_uploader("Upload one or more Excel files (.xlsx)", type=["xlsx"], accept_multiple_files=True)

    if uploads:
        all_rows = []
        errors = []
        batch_id = str(uuid.uuid4())[:8]

        for up in uploads:
            try:
                raw = pd.read_excel(up)
                miss = missing_fields_mask(raw).astype(int)

                apps = prepare_applications(raw)
                flags = apps.apply(eligibility_flags, axis=1)
                apps["is_disqualified"] = [f.is_disqualified for f in flags]
                apps["disqualify_reason"] = [f.disqualify_reason for f in flags]
                apps["needs_review"] = [f.needs_review for f in flags]
                apps["system_flags"] = [f.system_flags for f in flags]
                apps["missing_required_fields"] = miss.values
                apps["source_file"] = up.name
                apps["batch_id"] = batch_id

                all_rows.append(apps)
            except Exception as e:
                errors.append(f"{up.name}: {e}")

        if errors:
            st.error("Some files could not be processed:")
            st.write(errors)

        if all_rows:
            incoming = pd.concat(all_rows, ignore_index=True)

            # Check against existing DB (persons + issuances) but do not save incoming yet
            checked = compute_matches(incoming, persons_db, iss_db, threshold=name_threshold)
            checked["AutoStatus"] = ""
            checked["AutoReason"] = ""
            for i, r in checked.iterrows():
                stt, rsn = auto_status(r)
                checked.at[i, "AutoStatus"] = stt
                checked.at[i, "AutoReason"] = rsn

            # Create a stable RowID for follow-up corrections in Excel
            checked["RowID"] = checked.apply(lambda r: f"{batch_id}-{int(r.name)+1}", axis=1)

            s1, s2, s3, s4 = st.columns(4)
            s1.metric("AutoRejected", int((checked["AutoStatus"]=="REJECTED").sum()))
            s2.metric("NeedsFollowUp", int((checked["AutoStatus"]=="NEEDS_FOLLOW_UP").sum()))
            s3.metric("NeedsReview", int((checked["AutoStatus"]=="NEEDS_REVIEW").sum()))
            s4.metric("ApprovedReady", int((checked["AutoStatus"]=="APPROVED_READY").sum()))

            st.markdown("#### Preview (first 120)")
            st.dataframe(checked.head(120), width="stretch")

            review_df, roles = build_review_grouped(checked)
            excel_bytes = export_workbook(checked, review_df, roles)

            st.download_button(
                "Download results workbook (multi-sheet + corrections template)",
                data=excel_bytes,
                file_name=f"worldmap_batch_{batch_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # Store batch in session for committing later (without DB insert yet)
            st.session_state["last_checked_batch"] = checked

with tab2:
    st.subheader("Second check against DB + commit only truly clean rows")
    st.markdown("<div class='wm-card'><span class='wm-pill'>Safety</span><span class='wm-small'>Even after the first checks, we run a <b>second check</b> using the latest database before committing. Any row that matches a person in the DB (phone/ID/name similarity) will be held back and shown to the admin with the existing DB records.</span></div>", unsafe_allow_html=True)

    if "last_checked_batch" not in st.session_state:
        st.info("Run checks in tab 1 first (upload → check → export).")
    else:
        # Refresh DB snapshots (in case DB changed since tab 1)
        apps_db_now = fetch_applications(db_path)
        persons_db_now = fetch_persons(db_path)
        iss_db_now = fetch_issuances(db_path)

        checked = st.session_state["last_checked_batch"].copy()

        # Only rows marked clean from first pass
        clean_first = checked[checked["AutoStatus"]=="APPROVED_READY"].copy()
        st.metric("Rows marked clean (first pass)", 0 if clean_first.empty else len(clean_first))

        if clean_first.empty:
            st.info("No clean rows to commit from the last batch.")
        else:
            # SECOND CHECK: compute matches again against *current* DB
            second = compute_matches(clean_first, persons_db_now, iss_db_now, threshold=name_threshold).copy()

            # Decide if row is still clean (no dup phone/id, no prior issuance, no fuzzy matches)
            second["SecondCheck_HasMatch"] = False
            for i, r in second.iterrows():
                suspicious = bool(r.get("DuplicatePhone")) or bool(r.get("DuplicateID")) or bool(str(r.get("PriorIssuanceLatest","")).strip())
                name_sim = any(str(r.get(x,"")).strip() for x in ["TopMatch1","TopMatch2","TopMatch3"])
                second.at[i, "SecondCheck_HasMatch"] = bool(suspicious or name_sim)

            ok_to_commit = second[second["SecondCheck_HasMatch"]==False].copy()
            held = second[second["SecondCheck_HasMatch"]==True].copy()

            a, b = st.columns(2)
            a.metric("OK to commit after 2nd check", 0 if ok_to_commit.empty else len(ok_to_commit))
            b.metric("HELD for admin review", 0 if held.empty else len(held))

            st.markdown("### ✅ OK to commit (after 2nd check)")
            st.dataframe(ok_to_commit.head(120), width="stretch")

            st.markdown("### ⚠️ Held back (found matches in DB)")
            if held.empty:
                st.info("No held rows. Everything is still clean.")
            else:
                st.dataframe(held.head(120), width="stretch")

                # Pull existing DB person records for the held rows
                held_person_ids = set()
                # from phone/id direct matches (MatchedPersonID)
                for v in held.get("MatchedPersonID", pd.Series(dtype=str)).fillna("").astype(str).tolist():
                    if v.strip().isdigit():
                        held_person_ids.add(int(v.strip()))

                # from fuzzy top matches
                pid_lists = extract_person_ids_from_topmatches(held)
                for lst in pid_lists.tolist():
                    for pid in lst:
                        held_person_ids.add(pid)

                existing_persons = persons_db_now[persons_db_now["person_id"].isin(sorted(held_person_ids))].copy() if held_person_ids else pd.DataFrame()

                st.markdown("### 📌 Existing DB records that matched (Persons)")
                if existing_persons.empty:
                    st.info("No person rows extracted (unexpected). Check Match columns.")
                else:
                    st.dataframe(existing_persons, width="stretch")

                # Export held + existing DB records for admin
                exp = io.BytesIO()
                with pd.ExcelWriter(exp, engine="xlsxwriter") as w:
                    held.to_excel(w, index=False, sheet_name="Held_NewRows")
                    existing_persons.to_excel(w, index=False, sheet_name="Matched_DB_Persons")
                    # also include any DB issuances for those persons
                    if not existing_persons.empty:
                        pids = existing_persons["person_id"].astype(int).tolist()
                        iss = fetch_issuances(db_path, where="person_id IN (%s)" % ",".join(["?"]*len(pids)), params=tuple(pids))
                        iss.to_excel(w, index=False, sheet_name="Matched_DB_Issuances")
                st.download_button(
                    "Download held rows + matched DB records (Excel)",
                    data=exp.getvalue(),
                    file_name="worldmap_second_check_matches.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            st.markdown("---")
            st.markdown("### Commit")
            st.markdown("<div class='wm-card'><span class='wm-pill'>Commit</span><span class='wm-small'>Only rows in <b>OK to commit</b> will be inserted into the database as applications (status = APPROVED_READY).</span></div>", unsafe_allow_html=True)

            if st.button("Commit OK-to-commit rows now"):
                if ok_to_commit.empty:
                    st.warning("Nothing to commit.")
                else:
                    to_insert = ok_to_commit[[
                        "full_name_original","phone_original","national_id_original","country","church_name","title",
                        "congregation_size","requested_language","received_before","received_before_reason",
                        "full_name_normalized","name_key","phone_normalized","national_id_normalized",
                        "is_disqualified","disqualify_reason","needs_review","system_flags"
                    ]].copy()
                    to_insert["status"] = "APPROVED_READY"
                    source_label = str(ok_to_commit["batch_id"].iloc[0]) if "batch_id" in ok_to_commit.columns else "batch"
                    n = insert_applications(db_path, to_insert, source_file=f"batch:{source_label}")
                    st.success(f"Committed {n} application(s) into {db_path}. (Held rows were NOT committed.)")
                st.markdown("### Commit OVERRIDE rows (Admin)")

                if st.button("Commit OVERRIDE rows now (force commit)"):
                    if "override_for_commit" not in st.session_state or st.session_state["override_for_commit"].empty:
                        st.warning("No OVERRIDE rows prepared. Export HELD, fill AdminDecision=APPROVE_OVERRIDE + AdminOverrideReason, then re-upload.")
                    else:
                        ov = st.session_state["override_for_commit"].copy()

                        # Add a strong system flag that this was forced by admin
                        if "system_flags" in ov.columns:
                            ov["system_flags"] = ov["system_flags"].fillna("").astype(str) + " | ADMIN_OVERRIDE"
                        else:
                            ov["system_flags"] = "ADMIN_OVERRIDE"

                        # Map to insert schema (same columns as normal commit)
                        to_insert = ov[[
                            "full_name_original","phone_original","national_id_original","country","church_name","title",
                            "congregation_size","requested_language","received_before","received_before_reason",
                            "full_name_normalized","name_key","phone_normalized","national_id_normalized",
                            "is_disqualified","disqualify_reason","needs_review","system_flags"
                        ]].copy()

                        to_insert["status"] = "APPROVED_EXCEPTION"
                        batch_id = str(ov["batch_id"].iloc[0]) if "batch_id" in ov.columns else "batch"
                        n = insert_applications(db_path, to_insert, source_file=f"batch:{batch_id}")
                        st.success(f"Committed {n} OVERRIDE application(s) as APPROVED_EXCEPTION under batch {batch_id}.")

with tab3:

    st.subheader("Bulk issue from DB (APPROVED_READY only)")
    st.markdown("<div class='wm-card'><span class='wm-pill'>Issue</span><span class='wm-small'>This issues ONLY rows already committed to DB as APPROVED_READY.</span></div>", unsafe_allow_html=True)

    apps_db = fetch_applications(db_path)
    persons_db = fetch_persons(db_path)
    iss_db = fetch_issuances(db_path)

    ready = apps_db[apps_db["status"]=="APPROVED_READY"].copy() if not apps_db.empty else pd.DataFrame()
    st.metric("APPROVED_READY in DB", 0 if ready.empty else len(ready))

    if ready.empty:
        st.info("No APPROVED_READY rows in the DB yet. Commit a clean batch in tab 2.")
    else:
        use_requested = st.checkbox("Use each row's requested_language", value=True)
        global_lang = st.selectbox("If not using requested language, issue as:", ["KISWAHILI","ENGLISH","FRENCH"])
        issue_date = st.date_input("Issue date", value=date.today())

        st.dataframe(ready.head(120), width="stretch")

        if st.button("Bulk ISSUE all APPROVED_READY in DB"):
            issued = 0
            skipped = 0
            for _, row in ready.iterrows():
                aid = int(row["application_id"])

                # safeguard rules
                if int(row.get("congregation_size",0)) < 15:
                    set_application_status(db_path, aid, "REJECTED", admin_notes="AUTO: congregation < 15", matched_person_id=None)
                    skipped += 1
                    continue
                title = str(row.get("title","")).strip().upper()
                if title not in ALLOWED_TITLES:
                    set_application_status(db_path, aid, "REJECTED", admin_notes="AUTO: title not eligible", matched_person_id=None)
                    skipped += 1
                    continue

                # upsert person
                fn, mn, ln = split_name(str(row.get("full_name_normalized","")))
                rdict = row.to_dict()
                rdict["first_name"], rdict["middle_name"], rdict["last_name"] = fn, mn, ln
                person_id = upsert_person_from_application(db_path, rdict)

                # prior issuance check
                prior = fetch_issuances(db_path, where="person_id = ?", params=(person_id,))
                if not prior.empty:
                    set_application_status(db_path, aid, "NEEDS_REVIEW", admin_notes="AUTO: prior issuance found during bulk issue", matched_person_id=person_id)
                    skipped += 1
                    continue

                lang = str(row.get("requested_language","")).strip().upper() if use_requested else global_lang
                if lang not in {"KISWAHILI","ENGLISH","FRENCH"}:
                    lang = global_lang

                set_application_status(db_path, aid, "APPROVED", admin_notes="AUTO: bulk issued", matched_person_id=person_id)
                insert_issuance(
                    db_path,
                    person_id=person_id,
                    issued_at=issue_date.isoformat(),
                    language=lang,
                    issued_by=admin_user,
                    is_exception=0,
                    exception_type="",
                    exception_reason="",
                    notes="Bulk issue from APPROVED_READY",
                    batch_id=str(row.get("batch_id","")),
                )
                issued += 1

            st.success(f"Bulk issue complete. Issued: {issued} | Skipped/changed: {skipped}")


with tab4:
    st.subheader("Reports: Approved / Issued + Filters + Batches")
    st.markdown("<div class='wm-card'><span class='wm-pill'>Filters</span><span class='wm-small'>Filter approved/issued people and applications. Each committed import is a Batch. Duplicate checks always search the whole database.</span></div>", unsafe_allow_html=True)

    apps_db = fetch_applications(db_path)
    persons_db = fetch_persons(db_path)
    iss_db = fetch_issuances(db_path)
    batches = fetch_batches(db_path)

    # Latest issuance per person
    latest_issue = pd.DataFrame()
    if not iss_db.empty:
        iss_db_sorted = iss_db.sort_values("issued_at", ascending=False)
        latest_issue = iss_db_sorted.drop_duplicates(subset=["person_id"], keep="first")[["person_id","issued_at","language","issued_by","is_exception","exception_type","exception_reason","batch_id"]].copy()
        latest_issue = latest_issue.rename(columns={"issued_at":"latest_issued_at","language":"latest_language","batch_id":"issuance_batch_id"})

    people = persons_db.copy()
    if not latest_issue.empty:
        people = people.merge(latest_issue, on="person_id", how="left")

    # Filters row
    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        batch_opts = ["ALL"] + (batches["batch_id"].astype(str).tolist() if not batches.empty else [])
        batch_sel = st.selectbox("Batch", batch_opts, index=0)
    with f2:
        status_sel = st.selectbox("Application status", ["ALL","APPROVED_READY","APPROVED","REJECTED","NEEDS_REVIEW","NEEDS_FOLLOW_UP","PENDING"], index=0)
    with f3:
        country_opts = ["ALL"] + sorted([c for c in persons_db.get("country", pd.Series(dtype=str)).dropna().unique().tolist() if str(c).strip()])
        country_sel = st.selectbox("Country", country_opts, index=0)
    with f4:
        lang_sel = st.selectbox("Issued language", ["ALL","KISWAHILI","ENGLISH","FRENCH"], index=0)
    with f5:
        issued_sel = st.selectbox("Issued?", ["ALL","YES","NO"], index=0)
    search_q = st.text_input("Global search (name / phone / ID / church)", "")

    # Applications view
    st.markdown("### Applications (filtered)")
    apps_view = apps_db.copy() if not apps_db.empty else pd.DataFrame()
    if not apps_view.empty:
        if batch_sel != "ALL":
            apps_view = apps_view[apps_view.get("batch_id","").astype(str) == str(batch_sel)]
        if status_sel != "ALL":
            apps_view = apps_view[apps_view["status"] == status_sel]
    if "search_q" in locals() and search_q.strip() and not apps_view.empty:
        q = search_q.strip().lower()
        cols = [c for c in ["full_name_original","phone_original","national_id_original","church_name","country","title"] if c in apps_view.columns]
        if cols:
            mask = False
            for c in cols:
                mask = mask | apps_view[c].fillna("").astype(str).str.lower().str.contains(q)
            apps_view = apps_view[mask]

    if apps_view.empty:
        st.info("No applications match the filters.")
    else:
        st.dataframe(safe_df_for_display(apps_view).sort_values("submitted_at", ascending=False).head(500), width="stretch")

    # People view
    st.markdown("### People (with latest issuance)")
    people_view = people.copy()
    if not people_view.empty:
        if country_sel != "ALL":
            people_view = people_view[people_view.get("country","").astype(str).str.upper() == str(country_sel).upper()]
        if issued_sel != "ALL":
            ["latest_issued_at"].notna() if issued_sel=="YES" else people_view["latest_issued_at"].isna()
            # Above line is invalid; keep it simple below
    # Rebuild people view properly
    people_view = people.copy()
    if not people_view.empty:
        if country_sel != "ALL":
            people_view = people_view[people_view.get("country","").astype(str).str.upper() == str(country_sel).upper()]
        if issued_sel == "YES":
            people_view = people_view[people_view["latest_issued_at"].notna()]
        elif issued_sel == "NO":
            people_view = people_view[people_view["latest_issued_at"].isna()]
        if lang_sel != "ALL":
            people_view = people_view[people_view.get("latest_language","").astype(str).str.upper() == str(lang_sel).upper()]

    if "search_q" in locals() and search_q.strip() and not people_view.empty:
        q = search_q.strip().lower()
        cols = [c for c in ["full_name_normalized","phone_normalized","national_id_normalized","country","church_name"] if c in people_view.columns]
        if cols:
            mask = False
            for c in cols:
                mask = mask | people_view[c].fillna("").astype(str).str.lower().str.contains(q)
            people_view = people_view[mask]

    if people_view.empty:
        st.info("No people match the filters.")
    else:
        st.dataframe(safe_df_for_display(people_view).sort_values("latest_issued_at", ascending=False).head(500), width="stretch")

    
    st.markdown("### Export")
    if st.button("Download filtered results (Excel)"):
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as w:
            apps_view.to_excel(w, index=False, sheet_name="Applications_Filtered")
            people_view.to_excel(w, index=False, sheet_name="People_Filtered")
        st.download_button(
            "Download worldmap_filtered_export.xlsx",
            data=out.getvalue(),
            file_name="worldmap_filtered_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.markdown("### Batches (most recent first)")
    if batches.empty:
        st.info("No batches recorded yet. Commit a batch in tab 2 to create one.")
    else:
        st.dataframe(batches, width="stretch")
        bsel = st.selectbox("Open batch details", ["(none)"] + batches["batch_id"].astype(str).tolist(), index=0)
        if bsel != "(none)":
            a = apps_db[apps_db.get("batch_id","").astype(str) == str(bsel)].copy() if not apps_db.empty else pd.DataFrame()
            i = iss_db[iss_db.get("batch_id","").astype(str) == str(bsel)].copy() if not iss_db.empty else pd.DataFrame()
            st.markdown(f"#### Batch {bsel} details")
            st.markdown("**Applications in batch**")
            st.dataframe(a.sort_values("submitted_at", ascending=False).head(800), width="stretch")
            st.markdown("**Issuances in batch**")
            st.dataframe(i.sort_values("issued_at", ascending=False).head(800), width="stretch")

            exp = io.BytesIO()
            with pd.ExcelWriter(exp, engine="xlsxwriter") as w:
                a.to_excel(w, index=False, sheet_name="Batch_Applications")
                i.to_excel(w, index=False, sheet_name="Batch_Issuances")
                ppl = people[people["person_id"].isin(i["person_id"].unique().tolist())] if (not i.empty and not people.empty) else pd.DataFrame()
                ppl.to_excel(w, index=False, sheet_name="Batch_People")
            st.download_button(
                "Download this batch (Excel)",
                data=exp.getvalue(),
                file_name=f"worldmap_batch_{bsel}_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
