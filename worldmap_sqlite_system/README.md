# WorldMap – Shepherd Staff Distribution (Local SQLite)

## What this does
- Upload Excel applications (your exact columns)
- Flags eligibility rules (congregation size >= 15; title must be Pastor/Bishop/Evangelist/Bible School Overseer)
- Checks database history and flags suspected duplicates:
  - Phone duplicate
  - ID duplicate
  - Name similarity (e.g., Steve vs Stevie) with candidate matches shown for admin review
- Exports Excel with `Review_Grouped` (PRIMARY row then match candidates below; colored)
- Admin decisions create issuance history (exception approvals require reason)

## Install
```bash
pip install -r requirements.txt
```

## Run locally
```bash
streamlit run app.py
```

## Excel headers (must match exactly)
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

## Local DB
Default: `worldmap.db` (created automatically). You can change it in the sidebar.


## Bulk mode
- Upload multiple Excel files in one go (tab 1)
- Export filtered review or all review (tab 2)


## Batches & Reports
- Each committed import is recorded as a Batch (batch_id) in a batches table.
- Reports tab lets you filter by batch and see most recent batches first.
- Duplicate checks always search the whole database (all batches).

## Waterfall Stages (v1)
- **Stage 1:** Completeness (Title, Book Language, Congregation Size). Incomplete → NeedsFollowUp. Size<15 or Title not eligible → AutoRejected.
- **Stage 2:** Duplicate checks across the whole DB (Phone/ID), prior issuance, and name similarity tiers: HIGH (>=92) always flagged; MEDIUM (88–91) only when same country + last name + middle name match (flexible middle initial).
- Clean rows become **APPROVED_READY** and can be committed as a Batch.


## Stage 3 — Second Check Review Export/Import
- You can export HELD records during second check.
- Admin edits **Held_NewRows** sheet and fills **AdminDecision** (APPROVE / REJECT / FOLLOW_UP).
- Re-upload reviewed Excel.
- System re-runs second check on APPROVE rows before commit.
- Only clean rows are committed to DB.

## Admin override in Stage 3
- Export HELD records → edit in Excel (sheet Held_NewRows)
- Fill **AdminDecision** as one of: APPROVE / REJECT / FOLLOW_UP / APPROVE_OVERRIDE
- If you use **APPROVE_OVERRIDE**, you must also fill **AdminOverrideReason** (min 5 chars). The system will commit those rows as **APPROVED_EXCEPTION** and mark **ADMIN_OVERRIDE** in system_flags.


## Quality upgrades
- Reports tab: global search + export filtered results.
- Second Check: inline admin decisions using an editable grid (no Excel required).
- Safer dataframe display to avoid Arrow conversion errors.
