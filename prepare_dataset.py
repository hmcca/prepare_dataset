import polars as pl
import os
from pathlib import Path
import glob
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def read_text_file(file_path):
    """Read a single text file and return its content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return {'text': f.read(), 'path': file_path}
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def process_cxr_reports():
    """Process all text files in cxr_reports directory using parallel processing."""
    print("Finding all text files...")
    txt_files = list(glob.glob("cxr_reports/**/*.txt", recursive=True))
    total_files = len(txt_files)
    print(f"Found {total_files} files to process")
    
    # Process files in parallel using ThreadPoolExecutor
    results = []
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Use tqdm for progress bar
        for result in tqdm(executor.map(read_text_file, txt_files), total=total_files, desc="Processing CXR reports"):
            if result:
                results.append(result)
    
    # Convert to Polars DataFrame
    df = pl.DataFrame(results)
    
    # Calculate statistics
    word_count = df.select(
        pl.col("text").str.split(" ").list.len().sum()
    ).item()
    
    print(f"\nCXR Reports Statistics:")
    print(f"Total number of files processed: {len(results)}")
    print(f"Total number of words: {word_count}")
    
    return df.select("text")

def process_noteevents():
    """Process NOTEEVENTS.csv file using Polars."""
    print("\nProcessing NOTEEVENTS.csv...")
    
    # Read only the TEXT column using Polars with streaming
    df = pl.scan_csv('NOTEEVENTS.csv').select("TEXT").collect()
    
    # Calculate statistics
    row_count = df.height
    word_count = df.select(
        pl.col("TEXT").str.split(" ").list.len().sum()
    ).item()
    
    print(f"\nNOTEEVENTS Statistics:")
    print(f"Total number of rows: {row_count}")
    print(f"Total number of words: {word_count}")
    
    return df.rename({"TEXT": "text"})

def main():
    # Process CXR reports
    cxr_df = process_cxr_reports()
    
    # Process NOTEEVENTS
    notes_df = process_noteevents()
    
    # Combine both datasets efficiently
    print("\nCombining datasets...")
    combined_df = pl.concat([cxr_df, notes_df], how="vertical")
    
    print(f"\nCombined Statistics:")
    print(f"Total number of rows: {combined_df.height}")
    
    # Save to parquet with compression
    print("\nSaving to mimic.parquet...")
    combined_df.write_parquet(
        'mimic.parquet',
        compression="zstd",  # Use zstd compression for good compression/speed balance
        compression_level=3  # Moderate compression level
    )
    print("Done!")

if __name__ == "__main__":
    main()