"""
BHRC_newData.py - Rewritten for optimized secId handling

LOGIC OVERVIEW:
===============
1. Load secIds from CSV file (column index 98) and process in chunks of 900
2. Filter chunks to only include secIds within valid date range (inception to latest portfolio date)
3. Fetch holdings for all valid secIds in the chunk
4. RECURSIVELY expand mutual fund holdings up to depth 3:
   - For each MF holding with weight >= 0.01:
     a) PREFER the 'secId' column if it already contains a value
     b) ONLY convert 'isin' to secId if 'secId' is NULL/empty
     c) Batch ISIN conversions together to minimize API calls
   - Fetch underlying holdings for resolved secIds
   - Apply weight adjustments: underlying_weight = original_weight * parent_weight
   - Replace MF row with its underlying holdings
5. Save results to parquet files
6. Merge and clean files at the end of each date

KEY DIFFERENCES FROM ORIGINAL:
==============================
- NEW: Prioritizes existing 'secId' column over ISIN conversion
- NEW: resolve_secids_from_holdings() function handles the priority logic
- NEW: Batch collects all ISINs needing conversion before making API calls
- IMPROVED: Cleaner separation of concerns with dedicated helper functions
- IMPROVED: Better logging and documentation
- KEPT: Same weight adjustment logic, same file output structure, same DATE_RANGE loop


LOOK INTO COMPRESSION (SNAPPY ZSTD) LATER TO REDUCE FILE SIZE
"""

import morningstar_data as md
from morningstar_data import direct, mdapi
from morningstar_data.direct import InvestmentIdentifier
import pandas as pd
import os
from BHRC_tokens import API_KEY, DATE_RANGE
from BHRC_dataManagement import cleanFiles, delete_xholdings_inputs, merge_xholdings_by_date_arrow

os.environ['MD_AUTH_TOKEN'] = API_KEY

# Bump API read timeouts to reduce ReadTimeout errors on large pulls
mdapi._mdapi.MD_API_REQUEST_TIMEOUT = 15  # default was 5s
mdapi._mdapi.S3_READ_TIMEOUT = 60       # default was 60s

# ============================================================================
# GLOBAL CACHES
# ============================================================================
isin_conversion_fails = set()  # ISINs that failed conversion (O(1) lookup)
isin_to_secid_cache = {}       # Successful ISIN -> SecID mappings


# ============================================================================
# HOLDINGS DATE NORMALIZATION
# ============================================================================
def filter_holdings_by_date(df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """
    For each investmentId, keep rows at the requested portfolioDate if present.
    If absent, keep the latest portfolioDate <= target_date; if still none, keep the latest available.
    This avoids multi-date inflation of weights/marketValue.
    """
    if df.empty or 'portfolioDate' not in df.columns:
        return df

    df = df.copy()
    df['portfolioDate'] = pd.to_datetime(df['portfolioDate'], errors='coerce')
    target_ts = pd.to_datetime(target_date, errors='coerce')

    # Partition dates
    on_target = df['portfolioDate'] == target_ts
    if on_target.any():
        return df[on_target]

    # Latest <= target per investmentId
    df_sorted = df.sort_values(['investmentId', 'portfolioDate'])
    mask_le = df_sorted['portfolioDate'] <= target_ts
    le_candidates = df_sorted[mask_le]
    if not le_candidates.empty:
        idx_keep = le_candidates.groupby('investmentId')['portfolioDate'].transform('max') == le_candidates['portfolioDate']
        return le_candidates[idx_keep]

    # Fallback: latest available per investmentId
    latest_idx = df_sorted.groupby('investmentId')['portfolioDate'].transform('max') == df_sorted['portfolioDate']
    return df_sorted[latest_idx]


# ============================================================================
# BATCH ISIN TO SECID CONVERSION
# ============================================================================
def batch_convert_isins_to_secids(isins: list) -> dict:
    """
    Convert a list of ISINs to SecIDs in batch.
    Uses caching to avoid redundant API calls.
    
    Args:
        isins: List of ISIN strings to convert
        
    Returns:
        Dictionary mapping ISIN -> SecID for successful conversions
    """
    if not isins:
        return {}
    
    results = {}
    
    # First, retrieve any already-cached results
    isins_to_lookup = []
    for isin in isins:
        if isin in isin_to_secid_cache:
            results[isin] = isin_to_secid_cache[isin]
        elif isin not in isin_conversion_fails:
            isins_to_lookup.append(isin)
    
    # Lookup uncached ISINs via API
    for isin in isins_to_lookup:
        try:
            identifier = InvestmentIdentifier(isin=isin)
            secid_df = direct.lookup.investments(investment=identifier)
            
            if not secid_df.empty:
                secid = secid_df['SecId'].iloc[0]
                isin_to_secid_cache[isin] = secid
                results[isin] = secid
            else:
                isin_conversion_fails.add(isin)
        except Exception:
            isin_conversion_fails.add(isin)
    
    return results

# ============================================================================
# RESOLVE SECIDS FROM HOLDINGS DATAFRAME
# ============================================================================
def resolve_secids_from_holdings(mf_rows: pd.DataFrame) -> dict:
    """
    For mutual fund rows, resolve their SecIDs using priority:
    1. Use 'secId' column if available and non-empty
    2. Convert 'isin' to secId if secId is missing
    
    This minimizes API calls by batching ISIN conversions.
    
    Args:
        mf_rows: DataFrame containing MF holdings with 'secId' and 'isin' columns
        
    Returns:
        Dictionary mapping row index -> resolved SecID
    """
    index_to_secid = {}
    isin_to_indices = {}  # Track which row indices need each ISIN converted
    
    for idx, row in mf_rows.iterrows():
        # Check for existing secId first
        secid_val = row.get('secId')
        if pd.notna(secid_val) and str(secid_val).strip():
            index_to_secid[idx] = str(secid_val).strip()
            continue
        
        # Fall back to ISIN if secId is missing
        isin_val = row.get('isin')
        if pd.notna(isin_val) and str(isin_val).strip():
            isin = str(isin_val).strip()
            if isin not in isin_to_indices:
                isin_to_indices[isin] = []
            isin_to_indices[isin].append(idx)
    
    # Batch convert all ISINs that need conversion
    if isin_to_indices:
        unique_isins = list(isin_to_indices.keys())
        print(f"    Batch converting {len(unique_isins)} unique ISINs to SecIDs...")
        converted = batch_convert_isins_to_secids(unique_isins)
        print(f"    Successfully converted {len(converted)} ISINs")
        
        # Map converted SecIDs back to row indices
        for isin, secid in converted.items():
            for idx in isin_to_indices[isin]:
                index_to_secid[idx] = secid
    
    return index_to_secid

# ============================================================================
# RECURSIVE MUTUAL FUND EXPANSION
# ============================================================================
def recursive_expand_mutual_funds(
    df: pd.DataFrame, 
    chunk_number: int, 
    date: str, 
    max_depth: int = 3, 
    current_depth: int = 0
) -> pd.DataFrame:
    """
    Recursively expand mutual fund holdings up to max_depth.
    
    For each MF holding with weight >= 0.01:
    1. Resolve its SecID (prefer secId column, fallback to ISIN conversion)
    2. Fetch its underlying holdings
    3. Adjust weights proportionally
    4. Replace the MF row with its underlying holdings
    5. Recursively process any MFs in the new holdings
    
    Args:
        df: Holdings DataFrame
        chunk_number: Current chunk being processed (for logging)
        date: Portfolio date string
        max_depth: Maximum recursion depth (default 3)
        current_depth: Current recursion depth
        
    Returns:
        Expanded DataFrame with MF holdings replaced by underlying holdings
    """
    if current_depth >= max_depth:
        return df
    
    print(f"  DEPTH {current_depth}: Processing {len(df)} rows")
    
    # Define mutual fund types to expand
    MF_TYPES = [
        "MUTUAL FUND - OPEN END",
        "MUTUAL FUND - ETF",
        "MUTUAL FUND - CLOSED END",
    ]
    
    # Filter for mutual funds with significant weight (>= 0.01%)
    mask = (df['detailHoldingType'].isin(MF_TYPES)) & (df['weight'] >= 0.01)
    mf_rows = df[mask].copy()
    
    print(f"  DEPTH {current_depth}: Found {len(mf_rows)} mutual funds with weight >= 0.01")
    
    if mf_rows.empty:
        print(f"  DEPTH {current_depth}: No mutual funds to expand")
        return df
    
    # Log identifier availability
    has_secid = mf_rows['secId'].notna().sum() if 'secId' in mf_rows.columns else 0
    has_isin = mf_rows['isin'].notna().sum() if 'isin' in mf_rows.columns else 0
    print(f"  DEPTH {current_depth}: MF rows with secId: {has_secid}, with isin: {has_isin}")
    
    # Resolve SecIDs for all MF rows (prefer secId column, fallback to ISIN)
    index_to_secid = resolve_secids_from_holdings(mf_rows)
    
    if not index_to_secid:
        print(f"  DEPTH {current_depth}: Could not resolve any SecIDs")
        return df
    
    print(f"  DEPTH {current_depth}: Resolved {len(index_to_secid)} total SecIDs")
    
    # Collect unique SecIDs and fetch their holdings in batch
    unique_secids = list(set(index_to_secid.values()))
    
    # Fetch holdings in batches of 300 to avoid API limits
    all_holdings = []
    BATCH_SIZE = 900
    
    for i in range(0, len(unique_secids), BATCH_SIZE):
        batch_secids = unique_secids[i:i + BATCH_SIZE]
        try:
            batch_holdings = md.direct.get_holdings(investments=batch_secids, date=date)
            if isinstance(batch_holdings, pd.DataFrame) and not batch_holdings.empty:
                all_holdings.append(batch_holdings)
        except Exception as e:
            print(f"  DEPTH {current_depth}: Batch fetch error: {e}")
            # Fallback to individual fetching
            for secid in batch_secids:
                try:
                    holdings = md.direct.get_holdings(investments=[secid], date=date)
                    if isinstance(holdings, pd.DataFrame) and not holdings.empty:
                        all_holdings.append(holdings)
                except:
                    continue
    
    if not all_holdings:
        print(f"  DEPTH {current_depth}: No holdings retrieved")
        return df
    
    combined_holdings = pd.concat(all_holdings, ignore_index=True)
    # Keep only the requested date (or latest <= date) per investmentId to avoid multi-date inflation
    combined_holdings = filter_holdings_by_date(combined_holdings, date)
    
    # Add missing sharesChanged column if not present (can happen with certain funds/dates)
    if 'sharesChanged' not in combined_holdings.columns:
        combined_holdings['sharesChanged'] = 0.0
    
    print(f"  DEPTH {current_depth}: Fetched {len(combined_holdings)} total holding rows")
    
    # Add resolved SecID and a unique parent row identifier to mf_rows
    mf_rows = mf_rows.assign(
        resolvedSecId=mf_rows.index.map(index_to_secid),
        _parent_row_id=range(len(mf_rows))
    ).dropna(subset=['resolvedSecId'])

    if mf_rows.empty:
        print(f"  DEPTH {current_depth}: No resolvable MF rows after mapping")
        return df

    # Skip parents with near-100% weights to avoid circular references
    mf_rows = mf_rows[mf_rows['weight'] * 0.01 <= 0.9]
    if mf_rows.empty:
        print(f"  DEPTH {current_depth}: All MFs skipped due to weight > 90%")
        return df

    # Precompute fund-level total market values (once per fund, before any duplication)
    fund_totals = (
        combined_holdings.groupby('investmentId')['marketValue']
        .sum()
        .to_dict()
    )

    # Process each parent MF row individually to avoid cross-duplication
    expanded_rows = []
    successfully_expanded_indices = []

    for idx, parent_row in mf_rows.iterrows():
        secid = parent_row['resolvedSecId']
        parent_weight = parent_row['weight']
        parent_mv = parent_row['marketValue']
        og_portfolio = parent_row['investmentId']

        # Get holdings for this fund
        holdings_subset = combined_holdings[combined_holdings['investmentId'] == secid].copy()
        if holdings_subset.empty:
            continue

        # Compute correction: parent's stake / fund's total market value
        fund_total_mv = fund_totals.get(secid, 0)
        if fund_total_mv == 0:
            continue
        correction = parent_mv / fund_total_mv

        # Apply scaling
        holdings_subset['marketValue'] *= correction
        
        # Check for missing columns and diagnose
        if 'shares' not in holdings_subset.columns:
            print(f"  ⚠️  DEPTH {current_depth}: Missing 'shares' for secid={secid} (og_portfolio={og_portfolio})")
        elif holdings_subset['shares'].isna().all():
            print(f"  ⚠️  DEPTH {current_depth}: All 'shares' are NaN for secid={secid}")
        else:
            holdings_subset['shares'] *= correction
        
        if 'sharesChanged' not in holdings_subset.columns:
            print(f"  ⚠️  DEPTH {current_depth}: Missing 'sharesChanged' for secid={secid} (og_portfolio={og_portfolio})")
            print(f"       Available columns: {list(holdings_subset.columns)}")
        elif holdings_subset['sharesChanged'].isna().all():
            print(f"  ⚠️  DEPTH {current_depth}: All 'sharesChanged' are NaN for secid={secid}")
        else:
            holdings_subset['sharesChanged'] *= correction
        
        holdings_subset['weight'] *= parent_weight * 0.01
        holdings_subset['investmentId'] = og_portfolio

        expanded_rows.append(holdings_subset)
        successfully_expanded_indices.append(idx)

    if not expanded_rows:
        print(f"  DEPTH {current_depth}: No successful expansions")
        return df

    expanded_df = pd.concat(expanded_rows, ignore_index=True)
    print(f"  DEPTH {current_depth}: Successfully expanded {len(successfully_expanded_indices)} MF rows")

    # Remove expanded MF rows and add their underlying holdings
    df = df.drop(successfully_expanded_indices)
    df = pd.concat([df, expanded_df], ignore_index=True)
    
    # Recursive call for next depth level
    return recursive_expand_mutual_funds(
        df, chunk_number, date=date, 
        max_depth=max_depth, current_depth=current_depth + 1
    )

# ============================================================================
# FILTER SECIDS BY DATE RANGE
# ============================================================================
def filter_secids_by_date_range(secids: list, date: str) -> list:
    """
    Filter SecIDs to only include those whose inception-to-latest date range
    overlaps with the target date or 3 months prior.
    
    Args:
        secids: List of SecID strings
        date: Target date string (e.g., "2025-4-30")
        
    Returns:
        Filtered list of valid SecIDs
    """
    if not secids:
        return []
    
    # Parse target date
    date_parsed = pd.to_datetime(date, errors='coerce')
    date_minus_3m = date_parsed - pd.DateOffset(months=3)
    
    # Fetch inception dates
    try:
        inception_df = md.direct.get_investment_data(
            investments=secids,
            data_points=[
                {"datapointId": "OS01W"},  # Name
                {"datapointId": "OS00F"},  # Inception Date
            ],
        )
        inception_map = inception_df.set_index('Id')['Inception Date'].to_dict()
    except Exception as e:
        print(f"  Error fetching inception dates: {e}")
        inception_map = {}
    
    # Fetch latest reporting dates
    try:
        latest_df = md.direct.get_investment_data(
            investments=secids,
            data_points=[
                {"datapointId": "OS01W"},  # Name
                {"datapointId": "HS000"},  # Portfolio Date
            ],
        )
        latest_map = latest_df.set_index('Id')['Portfolio Date'].to_dict()
    except Exception as e:
        print(f"  Error fetching latest reporting dates: {e}")
        latest_map = {}
    
    # Filter SecIDs
    valid_secids = []
    
    for s in secids:
        inception_raw = inception_map.get(s)
        latest_raw = latest_map.get(s)
        
        if inception_raw is None or latest_raw is None:
            continue
        
        inception_dt = pd.to_datetime(inception_raw, errors='coerce')
        latest_dt = pd.to_datetime(latest_raw, errors='coerce')
        
        if pd.isna(inception_dt) or pd.isna(latest_dt):
            continue
        
        # Check if date range overlaps
        if (inception_dt <= date_parsed <= latest_dt) or (inception_dt <= date_minus_3m <= latest_dt):
            valid_secids.append(s)
    
    return valid_secids

# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================
def mainLoop(date: str, name: str, column: int, input_csv: str):
    """
    Main processing loop for a single date.
    
    1. Load SecIDs from CSV in chunks of 900
    2. Filter to valid date range
    3. Fetch holdings and recursively expand MFs
    4. Save results to parquet files
    """
    # Clear caches for each date to avoid stale data
    isin_conversion_fails.clear()
    isin_to_secid_cache.clear()
    
    # Configuration
    INPUT_CSV = input_csv
    NAME = name
    SECID_COLUMN_INDEX = column
    CHUNK_SIZE = 900
    
    # Load all SecIDs from CSV
    full_df = pd.read_csv(INPUT_CSV, header=0)
    all_secids = full_df.iloc[:, SECID_COLUMN_INDEX].tolist()
    total = len(all_secids)
    
    print(f"Total SecIDs in file: {total}")
    total_chunks = (total // CHUNK_SIZE) + (1 if total % CHUNK_SIZE > 0 else 0)
    print(f"Total chunks to process: {total_chunks}")
    
    # Open output log file
    with open(f"x_{name}-output-{date}.txt", "a") as log_file:
        
        for chunk_number in range(total_chunks):
            start_idx = chunk_number * CHUNK_SIZE
            end_idx = min((chunk_number + 1) * CHUNK_SIZE, total)
            
            print(f"\n{'='*60}")
            print(f"Chunk {chunk_number + 1}/{total_chunks}: SecIDs {start_idx} to {end_idx}.  date: {date}")
            print(f"{'='*60}")
            
            # Extract chunk of SecIDs
            chunk_secids = all_secids[start_idx:end_idx]
            
            # Remove NaN values
            chunk_secids = [s for s in chunk_secids if pd.notna(s)]
            print(f"Valid SecIDs in chunk: {len(chunk_secids)}")
            
            if not chunk_secids:
                print("No valid SecIDs, skipping chunk")
                continue
            
            # Filter by date range
            valid_secids = filter_secids_by_date_range(chunk_secids, date)
            print(f"SecIDs in valid date range: {len(valid_secids)}")
            
            if not valid_secids:
                print("No SecIDs in valid date range, skipping chunk")
                continue
            
            # Initialize result DataFrames
            all_MF_holdings = pd.DataFrame()
            no_recursion = pd.DataFrame()
            ID_fails = pd.DataFrame()
            fails = []
            
            try:
                print(f"Fetching holdings for {len(valid_secids)} SecIDs...")
                
                # Fetch initial holdings
                df = md.direct.get_holdings(investments=valid_secids, date=date)
                # Prevent multiple portfolioDate rows per investment
                df = filter_holdings_by_date(df, date)
                no_recursion = df.copy()
                
                # Recursively expand mutual fund holdings (max depth 3)
                df = recursive_expand_mutual_funds(
                    df, chunk_number, date=date, max_depth=3
                )
                
                all_MF_holdings = df.copy()
                
                # Track failures (SecIDs with no data)
                unique_investment_ids = set(all_MF_holdings['investmentId'].dropna().unique())
                fails = [s for s in valid_secids if s not in unique_investment_ids]
                
                if valid_secids:
                    failure_rate = len(fails) / len(valid_secids) * 100
                    print(f"Failed SecIDs: {len(fails)} ({failure_rate:.2f}%)")
                
                if fails:
                    ID_fails = pd.DataFrame({
                        'SecID': fails, 
                        'Error': ['No data retrieved'] * len(fails)
                    })
                
            except Exception as e:
                print(f"Error processing chunk: {e}")
                fails = valid_secids
            
            # Save results
            folder_path = fr"C:\Code\BHRC\date-{date}"
            os.makedirs(folder_path, exist_ok=True)
            
            # Drop unwanted columns before saving
            cols_to_drop = ["portfolioCurrency", "holdingsView", "holdingStorageId", "defaultHoldingTypeCode", "ticker"]
            all_MF_holdings = all_MF_holdings.drop(columns=cols_to_drop, errors="ignore")
            no_recursion = no_recursion.drop(columns=cols_to_drop, errors="ignore")

            all_MF_holdings.to_parquet(
                os.path.join(folder_path, f"x{NAME}-holdings-{date}_{chunk_number}.parquet"), 
                index=False
            )
            no_recursion.to_parquet(
                os.path.join(folder_path, f"x{NAME}-no_recursion-{date}_{chunk_number}.parquet"), 
                index=False
            )
            if not ID_fails.empty:
                ID_fails.to_parquet(
                    os.path.join(folder_path, f"x{NAME}-fails-{date}_{chunk_number}.parquet"), 
                    index=False
                )
            
            # Write summary to log
            log_file.write("-----------------------------------\n")
            log_file.write(f"Chunk {chunk_number + 1} results:\n")
            log_file.write(f"{len(fails)} failed SecIDs\n")
            log_file.write(f"{len(valid_secids)} total SecIDs\n")
            if valid_secids:
                log_file.write(f"{len(fails) / len(valid_secids) * 100:.2f}% failure rate\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    NAME = "1-5bil"
    INPUT_CSV = "1bil_excluding_5bil.csv"
    COLUMN = 0
    # 98 for 56852156178042AF9A2C89642148CCB1.csv
    # 77 for 10bil.csv
    # 0 for differences
    
    skip_first = True
    for date in DATE_RANGE:
        if skip_first:
            skip_first = False
            continue
        print(f"\n{'#'*70}")
        print(f"# Processing date: {date}")
        print(f"{'#'*70}")
        
        # Process all chunks for this date
        mainLoop(date, NAME, COLUMN, INPUT_CSV)
        print(f"Completed main processing loop for {date}.")
        # Merge all chunk parquet files into one
        merge_xholdings_by_date_arrow(name=NAME, date=date, base_dir=fr"C:\Code\BHRC\date-{date}")
        print("Completed merging xholdings files.")
        
        # Clean: remove rows that existed in previous date
        
        if date != DATE_RANGE[0]:
            print("Cleaning duplicate rows from previous date...")
            previous_date = DATE_RANGE[DATE_RANGE.index(date) - 1]
            cleanFiles(
                recent_file=fr"C:\Code\BHRC\date-{date}\xx{NAME}_MERGED_HOLDINGS_{date}.parquet",
                older_file=fr"C:\Code\BHRC\date-{previous_date}\xx{NAME}_MERGED_HOLDINGS_{previous_date}.parquet",
                out_file=fr"C:\Code\BHRC\date-{date}\xx{NAME}_CLEANED_{date}.parquet",
                delete_recent_after=False,
            )
        
        print(f"Completed processing date: {date}")

