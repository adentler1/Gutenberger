# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ebook manager that downloads public domain EPUBs from Project Gutenberg (with Internet Archive fallback), verifies metadata, analyzes reading difficulty (Flesch-Kincaid/CEFR), detects themes, and generates CSV catalogs. Books are organized by language and age/gender demographics via YAML config files.

## Commands

```bash
# Process all YAML categories (download, verify, analyze)
python Gutenberger.py

# Process a specific category
python Gutenberger.py 15-17_Jahre
python Gutenberger.py English_Boys_10-12

# Force re-analysis (ignore cache)
python Gutenberger.py --force

# Install dependencies
pip install pyyaml              # Required
pip install pyphen textstat     # Optional (better accuracy)
```

## Architecture

**Gutenberger.py**: All-in-one script that:
1. Reads YAML configs from script directory
2. Creates category folders (e.g., `English_Boys_10-12/`)
3. Downloads EPUBs from Gutenberg, falls back to Internet Archive
4. Validates EPUBs (ZIP format check, minimum 10KB size)
5. Extracts and verifies title/author from EPUB metadata
6. Calculates reading difficulty (Flesch-Kincaid grade → CEFR level)
7. Detects themes via keyword analysis (supports EN/DE/ES/FR)
8. Outputs `catalog.csv` in each category folder

**Incremental Processing**: Skips already-complete books (uses CSV as cache). Use `--force` to re-analyze.

**YAML Config Format**:
```yaml
category: "Category Name"
books:
  - title: "Book Title"
    author: "Author Name"
    filename: "Title_Author.epub"
    url: https://www.gutenberg.org/ebooks/ID.epub3.images
    gutenberg_id: ID
    note: "Optional description"
```

Use `gutenberg_id: 0` and `url: ""` for books not on Gutenberg (skipped) or to try Internet Archive fallback.

**Category Naming**:
- German: `*_Jahre.yaml` (12-14_Jahre, 15-17_Jahre, etc.), `Kindermärchen.yaml`, `Maedchen_*.yaml`
- English/Spanish: `{Language}_{Gender}_{AgeRange}.yaml` (e.g., `English_Boys_10-12.yaml`)
