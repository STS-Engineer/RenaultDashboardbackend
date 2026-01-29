from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import io
import csv
import time
from typing import List, Dict, Optional, Any
from statistics import median

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.db import get_conn, get_conn_bt

app = FastAPI()

# ---- CORS ----
origins = [
    "https://bt-renault.azurewebsites.net",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Sampling period (50 ms) => 0.05 s per row
SAMPLE_PERIOD_SEC = 0.05

# ---- Optional Excel support ----
try:
    import openpyxl  # noqa: F401
    HAS_OPENPYXL = True
except Exception:
    HAS_OPENPYXL = False


# -------------------------
# Robust decoding for CSV/TXT
# -------------------------
def _looks_utf16(raw: bytes) -> bool:
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return True
    head = raw[:4000]
    return head.count(b"\x00") > 50


def _decode_text(raw: bytes) -> str:
    if _looks_utf16(raw):
        for enc in ("utf-16", "utf-16le", "utf-16be"):
            try:
                return raw.decode(enc)
            except Exception:
                pass
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("latin1", errors="replace")


def _sniff_delimiter(line: str) -> str:
    if line.count(",") >= line.count(";"):
        return ","
    return ";"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().strip('"').strip("'") for c in df.columns]
    return df


# -------------------------
# Excel(1-col) "CSV in column A" parser
# -------------------------
def excel_one_col_csv_to_df(header_line: str, data_lines: List[str]) -> pd.DataFrame:
    header_line = str(header_line).strip()
    if not header_line:
        raise HTTPException(status_code=400, detail="Excel header line is empty.")

    delim = _sniff_delimiter(header_line)
    header = next(csv.reader([header_line], delimiter=delim, quotechar='"'))
    header = [h.strip().strip('"') for h in header]

    rows = []
    for ln in data_lines:
        ln = str(ln).strip()
        if not ln:
            continue
        parsed = next(csv.reader([ln], delimiter=delim, quotechar='"'))
        rows.append(parsed)

    if not rows:
        raise HTTPException(status_code=400, detail="Excel contains no data rows.")

    df = pd.DataFrame(rows, columns=header)
    return df


def read_csv_bytes(raw: bytes) -> pd.DataFrame:
    text = _decode_text(raw).replace("\x00", "")
    first_non_empty = ""
    for ln in text.splitlines():
        if ln.strip():
            first_non_empty = ln.strip()
            break
    if not first_non_empty:
        raise HTTPException(status_code=400, detail="Empty CSV/TXT file.")

    delim = _sniff_delimiter(first_non_empty)
    return pd.read_csv(io.StringIO(text), sep=delim, engine="python")


def read_uploaded(file: UploadFile, raw: bytes) -> pd.DataFrame:
    name = (file.filename or "").lower()

    # ---- Excel ----
    if name.endswith((".xlsx", ".xls")):
        if not HAS_OPENPYXL:
            raise HTTPException(
                status_code=400,
                detail="Excel read failed: Missing dependency 'openpyxl'. Run: pip install openpyxl",
            )

        try:
            df = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Excel read failed: {e}")

        # CASE: Excel sheet is actually "CSV text in column A"
        if df.shape[1] == 1:
            colname = str(df.columns[0])
            col_values = df.iloc[:, 0].astype(str).tolist()

            if ("Name_of_test" in colname) and (("," in colname) or (";" in colname)):
                return excel_one_col_csv_to_df(colname, col_values)

            if len(col_values) > 0:
                first_cell = str(col_values[0])
                if ("Name_of_test" in first_cell) and (("," in first_cell) or (";" in first_cell)):
                    return excel_one_col_csv_to_df(first_cell, col_values[1:])

        return df

    # ---- CSV/TXT ----
    return read_csv_bytes(raw)


# -------------------------
# Helpers (Python cleaning)
# -------------------------
def _clean_cell(v):
    """Convert pandas/CSV weird values to None where appropriate."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.upper() == "NONE":
            return None
        return s
    return v


def _to_float(v) -> Optional[float]:
    v = _clean_cell(v)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _detect_encoding_from_head(head: bytes) -> str:
    if _looks_utf16(head):
        return "utf-16"
    return "utf-8-sig"


# -------------------------
# Helpers (SQL cleaning + casting)
# -------------------------
def _clean_sql_float(col: str) -> str:
    return f"""
    CASE
      WHEN {col} IS NULL THEN NULL
      WHEN btrim({col}) = '' THEN NULL
      WHEN upper(btrim({col})) = 'NONE' THEN NULL
      WHEN btrim({col}) ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN (btrim({col}))::double precision
      ELSE NULL
    END
    """.strip()


# -------------------------
# Allowed fields for correlation (avoid SQL injection)
# -------------------------
ALLOWED_FIELDS: Dict[str, str] = {
    "DATA_BATTEMENT": "data_battement",
    "DATA_TEMPERATURE_SS_CONTACT": "data_temperature_ss_contact",
    "CONS_ALIM_1": "cons_alim_1",
    "CONS_ALIM_2": "cons_alim_2",
    "CONS_ALIM_3": "cons_alim_3",
    "CONSIGNE_TC": "consigne_tc",
    "RPM": "rpm",
    "SONDE_BRUSH_1_S1": "sonde_brush_1_s1",
    "SONDE_BRUSH_2_S1": "sonde_brush_2_s1",
    "SONDE_BRUSH_3_S1": "sonde_brush_3_s1",
    "SONDE_BRUSH_4_S1": "sonde_brush_4_s1",
    "SONDE_BRUSH_1_S2": "sonde_brush_1_s2",
    "SONDE_BRUSH_2_S2": "sonde_brush_2_s2",
    "SONDE_BRUSH_3_S2": "sonde_brush_3_s2",
    "SONDE_BRUSH_4_S2": "sonde_brush_4_s2",
    "SONDE_BRUSH_1_S3": "sonde_brush_1_s3",
    "SONDE_BRUSH_2_S3": "sonde_brush_2_s3",
    "SONDE_BRUSH_3_S3": "sonde_brush_3_s3",
    "SONDE_BRUSH_4_S3": "sonde_brush_4_s3",
    "SONDE_LOWER_1_S1": "sonde_lower_1_s1",
    "SONDE_LOWER_2_S1": "sonde_lower_2_s1",
    "SONDE_LOWER_1_S2": "sonde_lower_1_s2",
    "SONDE_LOWER_2_S2": "sonde_lower_2_s2",
    "SONDE_LOWER_1_S3": "sonde_lower_1_s3",
    "SONDE_LOWER_2_S3": "sonde_lower_2_s3",
    "SONDE_SUPPORT_S1": "sonde_support_s1",
    "SONDE_SUPPORT_S2": "sonde_support_s2",
    "SONDE_SUPPORT_S3": "sonde_support_s3",
    "CODEUR": "codeur",
    "TENSION1": "tension1",
    "TENSION2": "tension2",
    "TENSION3": "tension3",
}


# ✅ IMPORTANT FIX:
# Current in your data is effectively CONS_ALIM_1 for ALL systems.
# We keep system selection for voltage (TENSION1/2/3) + temps (S1/S2/S3),
# but CURRENT always comes from cons_alim_1 so the "Current vs time" chart never disappears.
def _system_cols(system: int) -> tuple[str, str]:
    """Return (cons_col, tension_col) for system 1/2/3."""
    if system not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="system must be 1, 2, or 3")
    return ("cons_alim_1", f"tension{system}")  # ✅ cons always from 1


# -------------------------
# Upload endpoint
# -------------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    filename = (file.filename or "").lower()

    # -----------------------------
    # Excel path (pandas -> VALUES)
    # -----------------------------
    if filename.endswith((".xlsx", ".xls")):
        raw = await file.read()
        if not raw:
            raise HTTPException(status_code=400, detail="Empty file.")

        df = read_uploaded(file, raw)
        df = normalize_columns(df)

        if "Name_of_test" not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Missing column: Name_of_test. Parsed columns: {df.columns.tolist()}",
            )

        test_series = df["Name_of_test"].astype(str).str.strip()
        test_series = test_series[test_series != ""]
        if test_series.empty:
            raise HTTPException(status_code=400, detail="Name_of_test is empty.")
        test_name = test_series.iloc[0]

        conn = get_conn()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                INSERT INTO tests(name)
                VALUES(%s)
                ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name
                RETURNING id
                """,
                (test_name,),
            )
            test_id = cur.fetchone()[0]

            cur.execute("DELETE FROM measurements WHERE test_id=%s", (test_id,))

            t0 = time.perf_counter()

            records: List[tuple] = []
            for i, r in df.iterrows():
                records.append((
                    test_id, int(i),

                    _to_float(r.get("DATA_BATTEMENT")),
                    _to_float(r.get("DATA_TEMPERATURE_SS_CONTACT")),
                    _to_float(r.get("CONS_ALIM_1")),
                    _to_float(r.get("CONS_ALIM_2")),
                    _to_float(r.get("CONS_ALIM_3")),
                    _to_float(r.get("CONSIGNE_TC")),
                    _to_float(r.get("RPM")),

                    _to_float(r.get("SONDE_BRUSH_1_S1")),
                    _to_float(r.get("SONDE_BRUSH_2_S1")),
                    _to_float(r.get("SONDE_BRUSH_3_S1")),
                    _to_float(r.get("SONDE_BRUSH_4_S1")),

                    _to_float(r.get("SONDE_BRUSH_1_S2")),
                    _to_float(r.get("SONDE_BRUSH_2_S2")),
                    _to_float(r.get("SONDE_BRUSH_3_S2")),
                    _to_float(r.get("SONDE_BRUSH_4_S2")),

                    _to_float(r.get("SONDE_BRUSH_1_S3")),
                    _to_float(r.get("SONDE_BRUSH_2_S3")),
                    _to_float(r.get("SONDE_BRUSH_3_S3")),
                    _to_float(r.get("SONDE_BRUSH_4_S3")),

                    _to_float(r.get("SONDE_LOWER_1_S1")),
                    _to_float(r.get("SONDE_LOWER_2_S1")),
                    _to_float(r.get("SONDE_LOWER_1_S2")),
                    _to_float(r.get("SONDE_LOWER_2_S2")),
                    _to_float(r.get("SONDE_LOWER_1_S3")),
                    _to_float(r.get("SONDE_LOWER_2_S3")),

                    _to_float(r.get("SONDE_SUPPORT_S1")),
                    _to_float(r.get("SONDE_SUPPORT_S2")),
                    _to_float(r.get("SONDE_SUPPORT_S3")),

                    _to_float(r.get("CODEUR")),
                    _to_float(r.get("TENSION1")),
                    _to_float(r.get("TENSION2")),
                    _to_float(r.get("TENSION3")),
                ))

            if not records:
                raise HTTPException(status_code=400, detail="No rows to import.")

            placeholders = "(" + ",".join(["%s"] * len(records[0])) + ")"
            args = ",".join(cur.mogrify(placeholders, rec).decode() for rec in records)

            cur.execute(
                """
                INSERT INTO measurements(
                  test_id, idx, data_battement, data_temperature_ss_contact,
                  cons_alim_1, cons_alim_2, cons_alim_3, consigne_tc, rpm,
                  sonde_brush_1_s1, sonde_brush_2_s1, sonde_brush_3_s1, sonde_brush_4_s1,
                  sonde_brush_1_s2, sonde_brush_2_s2, sonde_brush_3_s2, sonde_brush_4_s2,
                  sonde_brush_1_s3, sonde_brush_2_s3, sonde_brush_3_s3, sonde_brush_4_s3,
                  sonde_lower_1_s1, sonde_lower_2_s1,
                  sonde_lower_1_s2, sonde_lower_2_s2,
                  sonde_lower_1_s3, sonde_lower_2_s3,
                  sonde_support_s1, sonde_support_s2, sonde_support_s3,
                  codeur, tension1, tension2, tension3
                ) VALUES
                """ + args
            )

            conn.commit()

            t1 = time.perf_counter()
            elapsed_ms = round((t1 - t0) * 1000, 2)
            print(f"[UPLOAD TIMING][EXCEL] rows={len(records)} test='{test_name}' total={elapsed_ms}ms")

            return {"status": "ok", "rows": len(records), "test": test_name, "test_id": test_id, "timings": {"total_ms": elapsed_ms}}

        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=f"Excel upload failed: {e}")
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # --------------------------------
    # CSV/TXT path (COPY + INSERT)
    # --------------------------------
    total_t0 = time.perf_counter()

    try:
        file.file.seek(0)
    except Exception:
        pass

    head = file.file.read(4096)
    if not head:
        raise HTTPException(status_code=400, detail="Empty file.")

    encoding = _detect_encoding_from_head(head)

    head_text = head.decode(encoding, errors="replace").replace("\x00", "")
    first_non_empty = ""
    for ln in head_text.splitlines():
        if ln.strip():
            first_non_empty = ln.strip()
            break
    if not first_non_empty:
        raise HTTPException(status_code=400, detail="Empty CSV/TXT file.")

    delim = _sniff_delimiter(first_non_empty)
    file.file.seek(0)

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TEMP TABLE measurements_staging (
              data_battement text,
              data_temperature_ss_contact text,
              cons_alim_1 text,
              cons_alim_2 text,
              cons_alim_3 text,
              consigne_tc text,
              rpm text,
              sonde_brush_1_s1 text,
              sonde_brush_2_s1 text,
              sonde_brush_3_s1 text,
              sonde_brush_4_s1 text,
              sonde_brush_1_s2 text,
              sonde_brush_2_s2 text,
              sonde_brush_3_s2 text,
              sonde_brush_4_s2 text,
              sonde_brush_1_s3 text,
              sonde_brush_2_s3 text,
              sonde_brush_3_s3 text,
              sonde_brush_4_s3 text,
              sonde_lower_1_s1 text,
              sonde_lower_2_s1 text,
              sonde_lower_1_s2 text,
              sonde_lower_2_s2 text,
              sonde_lower_1_s3 text,
              sonde_lower_2_s3 text,
              sonde_support_s1 text,
              sonde_support_s2 text,
              sonde_support_s3 text,
              codeur text,
              name_of_test text,
              tension1 text,
              tension2 text,
              tension3 text,
              systeme_mesure text
            ) ON COMMIT DROP;
            """
        )

        text_stream = io.TextIOWrapper(file.file, encoding=encoding, errors="replace", newline="")

        copy_sql = f"""
            COPY measurements_staging(
              data_battement,
              data_temperature_ss_contact,
              cons_alim_1,
              cons_alim_2,
              cons_alim_3,
              consigne_tc,
              rpm,
              sonde_brush_1_s1,
              sonde_brush_2_s1,
              sonde_brush_3_s1,
              sonde_brush_4_s1,
              sonde_brush_1_s2,
              sonde_brush_2_s2,
              sonde_brush_3_s2,
              sonde_brush_4_s2,
              sonde_brush_1_s3,
              sonde_brush_2_s3,
              sonde_brush_3_s3,
              sonde_brush_4_s3,
              sonde_lower_1_s1,
              sonde_lower_2_s1,
              sonde_lower_1_s2,
              sonde_lower_2_s2,
              sonde_lower_1_s3,
              sonde_lower_2_s3,
              sonde_support_s1,
              sonde_support_s2,
              sonde_support_s3,
              codeur,
              name_of_test,
              tension1,
              tension2,
              tension3,
              systeme_mesure
            )
            FROM STDIN
            WITH (FORMAT csv, HEADER true, DELIMITER '{delim}', QUOTE '"')
        """

        t_copy0 = time.perf_counter()
        try:
            cur.copy_expert(copy_sql, text_stream)
        finally:
            try:
                text_stream.detach()
            except Exception:
                pass
        t_copy1 = time.perf_counter()

        cur.execute(
            """
            SELECT name_of_test
            FROM measurements_staging
            WHERE name_of_test IS NOT NULL AND btrim(name_of_test) <> ''
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row or not str(row[0]).strip():
            raise HTTPException(status_code=400, detail="Name_of_test is empty.")
        test_name = str(row[0]).strip()

        cur.execute(
            """
            INSERT INTO tests(name)
            VALUES(%s)
            ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name
            RETURNING id
            """,
            (test_name,),
        )
        test_id = cur.fetchone()[0]

        cur.execute("DELETE FROM measurements WHERE test_id=%s", (test_id,))

        insert_sql = f"""
            INSERT INTO measurements(
              test_id, idx,
              data_battement, data_temperature_ss_contact,
              cons_alim_1, cons_alim_2, cons_alim_3, consigne_tc, rpm,
              sonde_brush_1_s1, sonde_brush_2_s1, sonde_brush_3_s1, sonde_brush_4_s1,
              sonde_brush_1_s2, sonde_brush_2_s2, sonde_brush_3_s2, sonde_brush_4_s2,
              sonde_brush_1_s3, sonde_brush_2_s3, sonde_brush_3_s3, sonde_brush_4_s3,
              sonde_lower_1_s1, sonde_lower_2_s1,
              sonde_lower_1_s2, sonde_lower_2_s2,
              sonde_lower_1_s3, sonde_lower_2_s3,
              sonde_support_s1, sonde_support_s2, sonde_support_s3,
              codeur, tension1, tension2, tension3
            )
            SELECT
              %s AS test_id,
              (row_number() OVER ())::int - 1 AS idx,

              {_clean_sql_float("data_battement")},
              {_clean_sql_float("data_temperature_ss_contact")},

              {_clean_sql_float("cons_alim_1")},
              {_clean_sql_float("cons_alim_2")},
              {_clean_sql_float("cons_alim_3")},
              {_clean_sql_float("consigne_tc")},
              {_clean_sql_float("rpm")},

              {_clean_sql_float("sonde_brush_1_s1")},
              {_clean_sql_float("sonde_brush_2_s1")},
              {_clean_sql_float("sonde_brush_3_s1")},
              {_clean_sql_float("sonde_brush_4_s1")},

              {_clean_sql_float("sonde_brush_1_s2")},
              {_clean_sql_float("sonde_brush_2_s2")},
              {_clean_sql_float("sonde_brush_3_s2")},
              {_clean_sql_float("sonde_brush_4_s2")},

              {_clean_sql_float("sonde_brush_1_s3")},
              {_clean_sql_float("sonde_brush_2_s3")},
              {_clean_sql_float("sonde_brush_3_s3")},
              {_clean_sql_float("sonde_brush_4_s3")},

              {_clean_sql_float("sonde_lower_1_s1")},
              {_clean_sql_float("sonde_lower_2_s1")},
              {_clean_sql_float("sonde_lower_1_s2")},
              {_clean_sql_float("sonde_lower_2_s2")},
              {_clean_sql_float("sonde_lower_1_s3")},
              {_clean_sql_float("sonde_lower_2_s3")},

              {_clean_sql_float("sonde_support_s1")},
              {_clean_sql_float("sonde_support_s2")},
              {_clean_sql_float("sonde_support_s3")},

              {_clean_sql_float("codeur")},
              {_clean_sql_float("tension1")},
              {_clean_sql_float("tension2")},
              {_clean_sql_float("tension3")}

            FROM measurements_staging
            WHERE
            NOT (
                COALESCE(btrim(rpm),'')        ~ '^0*([.,]0*)?$'
                AND COALESCE(btrim(cons_alim_1),'') ~ '^0*([.,]0*)?$'
                AND COALESCE(btrim(tension1),'')    ~ '^0*([.,]0*)?$'
                AND COALESCE(btrim(tension2),'')    ~ '^0*([.,]0*)?$'
                AND COALESCE(btrim(tension3),'')    ~ '^0*([.,]0*)?$'
          )
       """

        t_ins0 = time.perf_counter()
        cur.execute(insert_sql, (test_id,))
        t_ins1 = time.perf_counter()

        cur.execute("SELECT COUNT(*) FROM measurements WHERE test_id=%s", (test_id,))
        nrows = int(cur.fetchone()[0])

        t_commit0 = time.perf_counter()
        conn.commit()
        t_commit1 = time.perf_counter()

        total_t1 = time.perf_counter()

        timings = {
            "copy_ms": round((t_copy1 - t_copy0) * 1000, 2),
            "insert_ms": round((t_ins1 - t_ins0) * 1000, 2),
            "commit_ms": round((t_commit1 - t_commit0) * 1000, 2),
            "total_ms": round((total_t1 - total_t0) * 1000, 2),
        }

        print(
            f"[UPLOAD TIMING][CSV] rows={nrows} test='{test_name}' "
            f"copy={timings['copy_ms']}ms insert={timings['insert_ms']}ms "
            f"commit={timings['commit_ms']}ms total={timings['total_ms']}ms"
        )

        return {"status": "ok", "rows": nrows, "test": test_name, "test_id": test_id, "timings": timings}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Upload failed: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# -------------------------
# Existing endpoints
# -------------------------
@app.get("/tests")
def tests():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM tests ORDER BY id DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"id": r[0], "name": r[1]} for r in rows]


# -------------------------
# Helpers for filtering + 150ms smoothing  (✅ SINGLE, FIXED COPY)
# -------------------------
def _is_zeroish(x: Optional[float], eps: float = 1e-12) -> bool:
    if x is None:
        return True
    try:
        return abs(float(x)) <= eps
    except Exception:
        return False


# ✅ FIXED: only check keys that exist in the row dict.
# This makes it compatible with BOTH:
# - /series rows (have vdrop)
# - /bt/series rows (do NOT have vdrop)
def _all_zero_row(row: Dict[str, Any]) -> bool:
    keys = ["rpm", "current_a", "vdrop", "b1", "b2", "b3", "b4", "l1", "l2", "sup"]
    present = [k for k in keys if k in row]
    return all(_is_zeroish(row.get(k)) for k in present)


def _rolling_median(values: List[Optional[float]], window: int = 3) -> List[Optional[float]]:
    if window <= 1:
        return values[:]
    half = window // 2
    out: List[Optional[float]] = []
    n = len(values)
    for i in range(n):
        a = max(0, i - half)
        b = min(n, i + half + 1)
        chunk = [v for v in values[a:b] if v is not None]
        out.append(median(chunk) if chunk else None)
    return out


def _apply_smoothing(rows: List[Dict[str, Any]], window: int = 3) -> List[Dict[str, Any]]:
    if not rows:
        return []  # ✅ keep your guard
    fields_to_smooth = ["vdrop", "current_a", "b1", "b2", "b3", "b4", "l1", "l2", "sup"]
    for f in fields_to_smooth:
        series = [r.get(f) for r in rows]
        sm = _rolling_median(series, window=window)
        for i, v in enumerate(sm):
            rows[i][f] = v
    return rows


# -------------------------
# FIXED /series endpoint
# -------------------------
@app.get("/series/{test_id}")
def series(
    test_id: int,
    system: int = Query(1, ge=1, le=3),
    step: int = Query(400, ge=1),
    dt_sec: float = Query(0.05, gt=0.0),
    t_start_sec: float = Query(0.0, ge=0.0),
    t_end_sec: float = Query(0.0, ge=0.0),
):
    cons_col = "cons_alim_1"
    sfx = f"s{system}"
    vdrop_col = f"tension{system}"

    start_idx = int(round(t_start_sec / dt_sec)) if t_start_sec > 0 else 0
    end_idx = None
    if t_end_sec and t_end_sec > 0:
        if t_end_sec < t_start_sec:
            raise HTTPException(status_code=400, detail="t_end_sec must be >= t_start_sec (or 0 for all).")
        end_idx = int(round(t_end_sec / dt_sec))

    conn = get_conn()
    cur = conn.cursor()

    try:
        where_time = "AND idx >= %s"
        params = [test_id, step, start_idx]
        if end_idx is not None:
            where_time += " AND idx <= %s"
            params.append(end_idx)

        sql = f"""
          SELECT
            idx,
            rpm,
            {cons_col} AS current_a,
            {vdrop_col} AS vdrop,
            tension1, tension2, tension3,

            sonde_brush_1_{sfx} AS b1,
            sonde_brush_2_{sfx} AS b2,
            sonde_brush_3_{sfx} AS b3,
            sonde_brush_4_{sfx} AS b4,

            sonde_lower_1_{sfx} AS l1,
            sonde_lower_2_{sfx} AS l2,
            sonde_support_{sfx} AS sup

          FROM measurements
          WHERE test_id=%s
            AND (idx %% %s) = 0
            {where_time}
          ORDER BY idx
        """

        cur.execute(sql, tuple(params))
        fetched = cur.fetchall()

        rows = []
        for r in fetched:
            row = {
                "idx": int(r[0]),
                "rpm": r[1],
                "current_a": r[2],
                "vdrop": r[3],
                "t1": r[4],
                "t2": r[5],
                "t3": r[6],
                "b1": r[7],
                "b2": r[8],
                "b3": r[9],
                "b4": r[10],
                "l1": r[11],
                "l2": r[12],
                "sup": r[13],
            }

            if row["rpm"] is not None and float(row["rpm"]) == 0.0:
                continue

            if _all_zero_row(row):
                continue

            t_sec = row["idx"] * float(dt_sec)
            row["t_sec"] = t_sec
            row["t_hour"] = t_sec / 3600.0
            rows.append(row)

        return _apply_smoothing(rows, window=3)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@app.get("/correlation/{test_id}")
def correlation(
    test_id: int,
    x: str = Query(...),
    y: str = Query(...),
    start: int = 0,
    length: int = 3000
):
    if x not in ALLOWED_FIELDS or y not in ALLOWED_FIELDS:
        raise HTTPException(status_code=400, detail=f"Invalid x/y. Allowed: {list(ALLOWED_FIELDS.keys())}")

    x_col = ALLOWED_FIELDS[x]
    y_col = ALLOWED_FIELDS[y]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
      SELECT {x_col}, {y_col}
      FROM measurements
      WHERE test_id=%s AND idx BETWEEN %s AND %s
    """, (test_id, start, start + length))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"x": r[0], "y": r[1]} for r in rows]


@app.get("/battement/{test_id}")
def battement(test_id: int, mode: str = "bol", n: int = 2000):
    conn = get_conn()
    cur = conn.cursor()
    if mode == "bol":
        cur.execute("""
          SELECT codeur, data_battement
          FROM measurements
          WHERE test_id=%s ORDER BY idx ASC LIMIT %s
        """, (test_id, n))
    else:
        cur.execute("""
          SELECT codeur, data_battement
          FROM measurements
          WHERE test_id=%s ORDER BY idx DESC LIMIT %s
        """, (test_id, n))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"angle": r[0], "battement": r[1]} for r in rows]


@app.get("/stats/{test_id}")
def stats(test_id: int, system: int = 1):
    cons_col, tension_col = _system_cols(system)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
      SELECT
        MIN(rpm), MAX(rpm),
        MIN({cons_col}), MAX({cons_col}),
        MIN({tension_col}), MAX({tension_col}),
        AVG(sonde_brush_1_s1)
      FROM measurements WHERE test_id=%s
    """, (test_id,))
    r = cur.fetchone()
    cur.close()
    conn.close()
    return {
        "rpm_min": r[0], "rpm_max": r[1],
        "a_min": r[2], "a_max": r[3],
        "v_min": r[4], "v_max": r[5],
        "t_avg": r[6]
    }


@app.get("/live_series/{test_id}")
def live_series_by_test_id(
    test_id: int,
    system: int = Query(1, ge=1, le=3),
    since_idx: int = Query(0, ge=0),
    step: int = Query(1),
    dt_sec: float = Query(0.05),
):
    cons_col = "cons_alim_1"
    vdrop_col = f"tension{system}"
    sfx = f"s{system}"

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(f"""
            SELECT
              idx,
              rpm,
              {cons_col} AS cons,
              {vdrop_col} AS vdrop,
              tension1, tension2, tension3,
              sonde_brush_1_{sfx},
              sonde_brush_2_{sfx},
              sonde_brush_3_{sfx},
              sonde_brush_4_{sfx},
              sonde_lower_1_{sfx},
              sonde_lower_2_{sfx},
              sonde_support_{sfx}
            FROM measurements
            WHERE test_id=%s
              AND idx > %s
              AND (idx %% %s)=0
            ORDER BY idx
            LIMIT 500
        """, (test_id, since_idx, step))

        rows = cur.fetchall()

        out = []
        for r in rows:
            idx = int(r[0])
            t_sec = idx * dt_sec
            out.append({
                "idx": idx,
                "t_sec": t_sec,
                "t_hour": t_sec / 3600.0,
                "rpm": r[1],
                "cons": r[2],
                "t1": r[4],
                "t2": r[5],
                "t3": r[6],
                "b1": r[7],
                "b2": r[8],
                "b3": r[9],
                "b4": r[10],
                "l1": r[11],
                "l2": r[12],
                "sup": r[13],
            })

        return out

    finally:
        cur.close()
        conn.close()


@app.get("/live_series")
def live_series_simple(
    test_id: int,
    system: int = 1,
    from_idx: int = 0,
    limit: int = 500,
    dt_sec: float = 0.05,
):
    cons_col = "cons_alim_1"
    vdrop_col = f"tension{system}"
    sfx = f"s{system}"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT
          idx,
          rpm,
          {cons_col} AS cons,
          tension1, tension2, tension3,
          sonde_brush_1_{sfx},
          sonde_brush_2_{sfx},
          sonde_brush_3_{sfx},
          sonde_brush_4_{sfx},
          sonde_lower_1_{sfx},
          sonde_lower_2_{sfx},
          sonde_support_{sfx}
        FROM measurements
        WHERE test_id = %s
          AND idx > %s
        ORDER BY idx
        LIMIT %s
    """, (test_id, from_idx, limit))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    out = []
    for r in rows:
        idx = int(r[0])
        t_sec = idx * dt_sec
        out.append({
            "idx": idx,
            "t_hour": t_sec / 3600.0,
            "rpm": r[1],
            "cons": r[2],
            "t1": r[3],
            "t2": r[4],
            "t3": r[5],
            "b1": r[6],
            "b2": r[7],
            "b3": r[8],
            "b4": r[9],
            "l1": r[10],
            "l2": r[11],
            "sup": r[12],
        })

    return out


@app.get("/bt/tests")
def bt_tests():
    return [{"id": 1, "name": "BT1"}, {"id": 2, "name": "BT2"}]


def _sql_safe_float(expr: str) -> str:
    return f"""
    CASE
      WHEN {expr} IS NULL THEN NULL
      WHEN btrim(({expr})::text) = '' THEN NULL
      WHEN upper(btrim(({expr})::text)) = 'NONE' THEN NULL
      WHEN btrim(({expr})::text) ~ '^[-+]?[0-9]*([\\.,][0-9]+)?$'
        THEN replace(btrim(({expr})::text), ',', '.')::double precision
      ELSE NULL
    END
    """.strip()


def _sql_safe_bigint(expr: str) -> str:
    return f"""
    CASE
      WHEN {expr} IS NULL THEN NULL
      WHEN btrim(({expr})::text) ~ '^[-+]?[0-9]+$'
        THEN btrim(({expr})::text)::bigint
      ELSE NULL
    END
    """.strip()


# -------------------------
# ✅ FIXED /bt/series endpoint
# -------------------------
@app.get("/bt/series/{test_id}")
def bt_series(
    test_id: int,
    system: int = Query(1, ge=1, le=3),
    step: int = Query(400, ge=1),
    dt_sec: float = Query(0.05, gt=0.0),
    t_start_sec: float = Query(0.0, ge=0.0),
    t_end_sec: float = Query(0.0, ge=0.0),
    after_idx: int = Query(-1, description="Return rows with idx > after_idx"),
    limit: int = Query(8000, ge=100, le=50000),
):
    if test_id == 1:
        table = '"Banc_Test_Rotor"."BT1"'
    elif test_id == 2:
        table = '"Banc_Test_Rotor"."BT2"'
    else:
        raise HTTPException(status_code=400, detail="test_id must be 1 (BT1) or 2 (BT2)")

    sfx = f"S{system}"

    start_idx = int(round(t_start_sec / dt_sec)) if t_start_sec > 0 else 0
    end_idx = None
    if t_end_sec and t_end_sec > 0:
        if t_end_sec < t_start_sec:
            raise HTTPException(status_code=400, detail="t_end_sec must be >= t_start_sec (or 0 for all).")
        end_idx = int(round(t_end_sec / dt_sec))

    where_parts = [
        "idx IS NOT NULL",
        "idx >= %(start)s",
        "(idx % %(step)s) = 0",
        "idx > %(after)s",
    ]
    params = {
        "start": start_idx,
        "step": step,
        "after": after_idx,
        "limit": limit,
    }
    if end_idx is not None:
        where_parts.append("idx <= %(end)s")
        params["end"] = end_idx

    where_sql = " AND ".join(where_parts)

    sql = f"""
      SELECT
        idx,
        "RPM" AS rpm,
        "CONS_ALIM_1" AS current_a,
        "TENSION1" AS t1, "TENSION2" AS t2, "TENSION3" AS t3,

        "SONDE_BRUSH_1_{sfx}" AS b1,
        "SONDE_BRUSH_2_{sfx}" AS b2,
        "SONDE_BRUSH_3_{sfx}" AS b3,
        "SONDE_BRUSH_4_{sfx}" AS b4,

        "SONDE_LOWER_1_{sfx}" AS l1,
        "SONDE_LOWER_2_{sfx}" AS l2,
        "SONDE_SUPPORT_{sfx}" AS sup
      FROM {table}
      WHERE {where_sql}
      ORDER BY idx
      LIMIT %(limit)s
    """

    conn = get_conn_bt()
    cur = conn.cursor()
    try:
        cur.execute("SET statement_timeout TO '30s';")
        try:
            cur.execute(sql, params)
        except Exception as e:
            # ✅ shows DB error in Swagger instead of generic "Internal Server Error"
            raise HTTPException(status_code=500, detail=str(e))

        fetched = cur.fetchall()

        rows = []
        for r in fetched:
            row = {
                "idx": int(r[0]),
                "rpm": r[1],
                "current_a": r[2],
                "t1": r[3],
                "t2": r[4],
                "t3": r[5],
                "b1": r[6],
                "b2": r[7],
                "b3": r[8],
                "b4": r[9],
                "l1": r[10],
                "l2": r[11],
                "sup": r[12],
            }

            if row["rpm"] is not None and float(row["rpm"]) == 0.0:
                continue
            if _all_zero_row(row):
                continue

            t_sec = row["idx"] * float(dt_sec)
            row["t_sec"] = t_sec
            row["hours"] = t_sec / 3600.0

            rows.append(row)

        return _apply_smoothing(rows, window=3)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
