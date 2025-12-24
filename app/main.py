from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import io
import csv
from typing import List, Dict

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.db import get_conn

app = FastAPI()

# ---- CORS ----
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# Helpers
# -------------------------
def _clean_cell(v):
    """Convert pandas/CSV weird values to None where appropriate."""
    if v is None:
        return None
    # NaN handling
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    # Strings like NONE / empty -> NULL
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.upper() == "NONE":
            return None
        return s
    return v


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


# -------------------------
# Upload endpoint
# -------------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
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

    # 1) get (or create) test_id by name
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

    # ✅ FIX: overwrite on re-upload (prevents duplicates across uploads)
    # This does NOT deduplicate rows inside the same file; it only replaces previous imports.
    cur.execute("DELETE FROM measurements WHERE test_id=%s", (test_id,))

    records: List[tuple] = []
    for i, r in df.iterrows():
        records.append((
            test_id, int(i),
            _clean_cell(r.get("DATA_BATTEMENT")), _clean_cell(r.get("DATA_TEMPERATURE_SS_CONTACT")),
            _clean_cell(r.get("CONS_ALIM_1")), _clean_cell(r.get("CONS_ALIM_2")), _clean_cell(r.get("CONS_ALIM_3")),
            _clean_cell(r.get("CONSIGNE_TC")), _clean_cell(r.get("RPM")),
            _clean_cell(r.get("SONDE_BRUSH_1_S1")), _clean_cell(r.get("SONDE_BRUSH_2_S1")),
            _clean_cell(r.get("SONDE_BRUSH_3_S1")), _clean_cell(r.get("SONDE_BRUSH_4_S1")),
            _clean_cell(r.get("SONDE_BRUSH_1_S2")), _clean_cell(r.get("SONDE_BRUSH_2_S2")),
            _clean_cell(r.get("SONDE_BRUSH_3_S2")), _clean_cell(r.get("SONDE_BRUSH_4_S2")),
            _clean_cell(r.get("SONDE_BRUSH_1_S3")), _clean_cell(r.get("SONDE_BRUSH_2_S3")),
            _clean_cell(r.get("SONDE_BRUSH_3_S3")), _clean_cell(r.get("SONDE_BRUSH_4_S3")),
            _clean_cell(r.get("SONDE_LOWER_1_S1")), _clean_cell(r.get("SONDE_LOWER_2_S1")),
            _clean_cell(r.get("SONDE_LOWER_1_S2")), _clean_cell(r.get("SONDE_LOWER_2_S2")),
            _clean_cell(r.get("SONDE_LOWER_1_S3")), _clean_cell(r.get("SONDE_LOWER_2_S3")),
            _clean_cell(r.get("SONDE_SUPPORT_S1")), _clean_cell(r.get("SONDE_SUPPORT_S2")), _clean_cell(r.get("SONDE_SUPPORT_S3")),
            _clean_cell(r.get("CODEUR")),
            _clean_cell(r.get("TENSION1")), _clean_cell(r.get("TENSION2")), _clean_cell(r.get("TENSION3")),
        ))

    if not records:
        raise HTTPException(status_code=400, detail="No rows to import.")

    # dynamic placeholders (no manual counting)
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
    cur.close()
    conn.close()

    return {"status": "ok", "rows": len(records), "test": test_name, "test_id": test_id}


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


@app.get("/series/{test_id}")
def series(test_id: int, module: int = 1, step: int = 100):
    if module not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="module must be 1, 2, or 3")

    s = f"s{module}"  # "s1" / "s2" / "s3"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"""
      SELECT
        idx,
        rpm,
        cons_alim_1,
        tension1, tension2, tension3,

        sonde_brush_1_{s}, sonde_brush_2_{s}, sonde_brush_3_{s}, sonde_brush_4_{s},
        sonde_lower_1_{s}, sonde_lower_2_{s},
        sonde_support_{s}

      FROM measurements
      WHERE test_id=%s AND idx %% %s = 0
      ORDER BY idx
    """, (test_id, step))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [{
        "idx": r[0],
        "rpm": r[1],
        "cons": r[2],
        "t1": r[3], "t2": r[4], "t3": r[5],
        "b1": r[6], "b2": r[7], "b3": r[8], "b4": r[9],
        "l1": r[10], "l2": r[11],
        "sup": r[12],
    } for r in rows]


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
def stats(test_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT
        MIN(rpm), MAX(rpm),
        MIN(cons_alim_1), MAX(cons_alim_1),
        MIN(tension1), MAX(tension1),
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
