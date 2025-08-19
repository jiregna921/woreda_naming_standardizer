# standardizer.py
import pandas as pd
from rapidfuzz import process, fuzz

def match_and_merge_two_datasets(df1, df2, col_mapping1, col_mapping2, region_threshold, zone_threshold, woreda_threshold):
    """
    Fuzzy matches and merges two datasets based on a list of key columns.
    
    Parameters:
        df1 (DataFrame): The first dataset (left side of the merge).
        df2 (DataFrame): The second dataset (right side of the merge).
        col_mapping1 (dict): A dictionary mapping required keys to actual column names in df1.
        col_mapping2 (dict): A dictionary mapping required keys to actual column names in df2.
        region_threshold (int): Match score threshold for Region.
        zone_threshold (int): Match score threshold for Zone.
        woreda_threshold (int): Match score threshold for Woreda.

    Returns:
        DataFrame: A merged dataframe with all columns from both inputs.
        DataFrame: A dataframe with unmatched rows from df1.
        DataFrame: A dataframe with unmatched rows from df2.
    """
    # Create copies of the dataframes to avoid modifying the originals
    df1_copy = df1.copy()
    df2_copy = df2.copy()
    
    # Standardize column names based on the provided mapping
    for req_col, actual_col in col_mapping1.items():
        if actual_col != req_col:
            df1_copy.rename(columns={actual_col: req_col}, inplace=True)
            df1_copy[req_col] = df1_copy[req_col].astype(str).str.strip().str.lower()
            
    for req_col, actual_col in col_mapping2.items():
        if actual_col != req_col:
            df2_copy.rename(columns={actual_col: req_col}, inplace=True)
            df2_copy[req_col] = df2_copy[req_col].astype(str).str.strip().str.lower()
    
    # Define the key columns and non-key columns from df2 to be merged
    key_cols = list(col_mapping1.keys())
    df2_non_key_cols = [col for col in df2.columns if col not in col_mapping2.values()]
    
    # Prepare for merging and tracking unmatched rows
    matched_rows = []
    unmatched_df1_rows = []
    matched_indices_df2 = set()
    
    # Iterate through df1 and find matches in df2
    for index1, row1 in df1_copy.iterrows():
        found_match = False
        
        # Iterate through df2 to find a match for the current row from df1
        for index2, row2 in df2_copy.iterrows():
            if index2 in matched_indices_df2:
                continue # Skip if this row from df2 is already matched
                
            region_score = fuzz.ratio(str(row1['region']), str(row2['region']))
            zone_score = fuzz.ratio(str(row1['zone']), str(row2['zone']))
            woreda_score = fuzz.token_set_ratio(str(row1['woreda']), str(row2['woreda']))

            if (region_score >= region_threshold and 
                zone_score >= zone_threshold and 
                woreda_score >= woreda_threshold):
                
                # A match is found, prepare the combined row
                combined_row = df1.iloc[index1].to_dict()
                
                # Add only the non-key columns from df2 to the combined row
                for col in df2_non_key_cols:
                    combined_row[col] = df2.iloc[index2][col]
                    
                matched_rows.append(combined_row)
                matched_indices_df2.add(index2)
                found_match = True
                break # Move to the next row in df1
        
        if not found_match:
            unmatched_df1_rows.append(df1.iloc[index1].to_dict())

    merged_df = pd.DataFrame(matched_rows)
    unmatched_df1 = pd.DataFrame(unmatched_df1_rows)

    unmatched_indices_df2 = [i for i in range(len(df2)) if i not in matched_indices_df2]
    unmatched_df2 = df2.iloc[unmatched_indices_df2]

    return merged_df, unmatched_df1, unmatched_df2
