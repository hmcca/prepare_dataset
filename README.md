# data_prep_mimc

## Overview
This repository provides scripts and instructions to aggregate and preprocess two large clinical text datasets—CXR radiology reports and MIMIC-III NOTEEVENTS—into a single unified Parquet file suitable for pretraining large language models (LLMs).

## Dataset Structure

### 1. CXR Reports (`cxr_reports/`)
- **Format:** Nested directory structure containing plain text files (`.txt`).
- **Each file:** Represents a single radiology report.
- **Example path:** `cxr_reports/p10/p10000032/s50414267.txt`
- **Example content:**
  ```
  FINAL REPORT
  EXAMINATION:  CHEST (PA AND LAT)
  ...
  IMPRESSION: 
  No acute cardiopulmonary process.
  ```

### 2. NOTEEVENTS (`NOTEEVENTS.csv`)
- **Format:** Large CSV file from MIMIC-III.
- **Relevant column:** `TEXT` (contains the clinical note text)
- **Other columns:** ROW_ID, SUBJECT_ID, HADM_ID, CHARTDATE, CHARTTIME, STORETIME, CATEGORY, DESCRIPTION, CGID, ISERROR (ignored in this processing)

## Data Processing & Statistics

The script `prepare_dataset.py` performs the following steps:
1. **CXR Reports:**
   - Recursively reads all `.txt` files in `cxr_reports/`.
   - Each file's content is treated as a separate row.
   - **Stats:**
     - Total files processed: **227,835**
     - Total words: **30,661,310**
2. **NOTEEVENTS:**
   - Reads only the `TEXT` column from `NOTEEVENTS.csv` using Polars for efficiency.
   - **Stats:**
     - Total rows: **2,083,180**
     - Total words: **797,752,362**
3. **Combining:**
   - Both datasets are concatenated into a single DataFrame with a single column: `text`.
   - The result is saved as a compressed Parquet file: `mimic.parquet`.
   - **Final stats:**
     - Total rows: **2,311,015**
     - File size: ~927MB (compressed with zstd)

## Usage

### Requirements
- Python 3.8+
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```

### Running the Script
```bash
python prepare_dataset.py
```
This will:
- Process all CXR report files in parallel
- Efficiently process NOTEEVENTS.csv
- Output a single `mimic.parquet` file with all text data in a single column
- Print summary statistics

## Output
- **mimic.parquet**: Unified dataset with a single column `text` containing all clinical notes and reports.

## Repository Structure
```
├── cxr_reports/         # Nested folders of .txt files (radiology reports)
├── NOTEEVENTS.csv       # MIMIC-III NOTEEVENTS file
├── mimic.parquet        # Output: unified, compressed parquet file
├── prepare_dataset.py   # Main processing script
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Notes
- The script uses Polars for high performance and low memory usage.
- CXR reports are processed in parallel for speed.
- The output is suitable for LLM pretraining or further downstream NLP tasks.
