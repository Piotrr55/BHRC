from __future__ import annotations
#if weird errors come maybe the first 2 funcs are incompatible with this import

from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from BHRC_tokens import DATE, DATE_RANGE
import os
import duckdb

def merge_xholdings_by_date_arrow(name: str,date: str, base_dir: str = "."):
    """
    Memory-efficient merge of parquet files using PyArrow.
    Safely skips empty parquet files.
    """
    folder = Path(base_dir)
    output_file = folder / f"xx{name}_MERGED_HOLDINGS_{date}.parquet"

    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    parquet_files = sorted(folder.glob(f"x{name}-holdings-{date}_*.parquet"))

    if not parquet_files:
        raise FileNotFoundError("No parquet files found to merge.")

    tables = []
    skipped_empty = 0

    # Find a reference schema from the first non-empty file
    reference_schema = None
    for file in parquet_files:
        pf = pq.ParquetFile(file)
        if pf.metadata.num_rows > 0:
            reference_schema = pf.schema_arrow
            break

    if reference_schema is None:
        # All files are empty → write empty parquet with no rows
        empty_table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in pq.read_schema(parquet_files[0])],
            schema=pq.read_schema(parquet_files[0])
        )
        pq.write_table(empty_table, output_file)
        print(f"✅ All input files empty. Wrote empty parquet → {output_file}")
        return

    for file in parquet_files:
        pf = pq.ParquetFile(file)

        if pf.metadata.num_rows == 0:
            skipped_empty += 1
            continue

        table = pq.read_table(file)

        # Align schema if needed (reorder columns and cast types)
        if not table.schema.equals(reference_schema):
            # Reorder columns to match reference schema
            table = table.select([field.name for field in reference_schema])
            # Then cast to ensure type compatibility
            table = table.cast(reference_schema)

        tables.append(table)

    combined_table = pa.concat_tables(tables)
    pq.write_table(combined_table, output_file)

    print(
        f"✅ Merged {len(tables)} files into {output_file} "
        f"(skipped {skipped_empty} empty files)"
    )

def delete_xholdings_inputs(name: str, date: str, base_dir: str = "."):
    """
    Delete input parquet files for a given date after final file is confirmed.
    """
    folder = Path(base_dir)
    final_file = folder / f"xx{name}_MERGED_HOLDINGS_{date}.parquet"

    if not final_file.exists():
        raise RuntimeError(
            f"Final parquet not found, refusing to delete inputs: {final_file}"
        )

    input_files = sorted(folder.glob(f"x{name}-holdings-{date}_*.parquet"))

    deleted = 0
    for file in input_files:
        try:
            file.unlink()
            deleted += 1
        except Exception as e:
            print(f"⚠️ Could not delete {file}: {e}")

    print(f"🧹 Deleted {deleted} input parquet files for date {date}")

def delete_file(path: str | os.PathLike, *, missing_ok: bool = False) -> None:
    """Delete a file from disk."""
    p = Path(path)
    try:
        p.unlink()
    except FileNotFoundError:
        if not missing_ok:
            raise

def cleanFiles(
    recent_file: str | os.PathLike,
    older_file: str | os.PathLike,
    out_file: str | os.PathLike,
    *,
    keys: tuple[str, ...] = ("portfolioDate", "investmentId", "masterPortfolioId"),
    delete_recent_after: bool = False,
):
    """
    Write out rows from A whose (keys) do NOT appear in B.
    Keeps all columns from A.
    """
    recent_file = str(Path(recent_file))
    older_file = str(Path(older_file))
    out_file = str(Path(out_file))

    con = duckdb.connect(database=":memory:")

    rows_recent = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{recent_file}')"
    ).fetchone()[0]
    # Build join condition: handle NULLs safely by treating NULL = NULL as match
    # (important if any key columns can be NULL).
    cond = " AND ".join([f"(a.{k} IS NOT DISTINCT FROM b.{k})" for k in keys])

    query = f"""
    COPY (
        SELECT a.*
        FROM read_parquet('{recent_file}') a
        WHERE NOT EXISTS (
            SELECT 1
            FROM (SELECT DISTINCT {", ".join(keys)} FROM read_parquet('{older_file}')) b
            WHERE {cond}
        )
    )
    TO '{out_file}' (FORMAT PARQUET);
    """

    con.execute(query)

    rows_out = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_file}')"
    ).fetchone()[0]

    con.close()

    print(f"Rows in A: {rows_recent}")
    print(f"Rows in output: {rows_out}")
    print(f"Rows deleted: {rows_recent - rows_out}")
    
    if rows_recent - rows_out == 0:
        print("✅ no rows were deleted. returning same file as recent_file")
        delete_file(out_file, missing_ok=True)
        return

    if delete_recent_after:
        delete_file(recent_file)
        print(f"🗑️ Deleted recent file: {recent_file}")

    print(
        f"✅ Wrote rows from {recent_file} not in {older_file} to {out_file}"
    )

