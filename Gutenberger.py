#!/usr/bin/env python3
"""
Ebook Manager - All-in-one ebook downloader, verifier, and analyzer

This script:
1. Finds all YAML config files in the current directory
2. Creates subfolders named after each YAML file (without extension)
3. Downloads missing EPUBs from Project Gutenberg (with Internet Archive fallback)
4. Verifies downloaded files match expected metadata
5. Analyzes reading difficulty (Flesch-Kincaid/CEFR)
6. Detects themes
7. Outputs a CSV file in each subfolder with all metadata

Usage:
    python ebook_manager.py                    # Process all YAML files
    python ebook_manager.py my_books           # Process specific YAML file
    python ebook_manager.py --force            # Re-analyze all books (ignore cache)

Requirements:
    pip install pyyaml pyphen textstat

================================================================================
LLM PROMPT FOR GENERATING YAML CONFIG FILES
================================================================================

Use the following prompt with an LLM (Claude, GPT, etc.) to generate a YAML
config file for downloading ebooks:

--- START PROMPT ---

Create a YAML configuration file for downloading public domain ebooks.

The file must follow this exact format:

```yaml
category: "Category Name Here"
books:
  - title: "Full Book Title"
    author: "Author Full Name"
    filename: "Short_Title_Author.epub"
    url: https://www.gutenberg.org/ebooks/XXXXX.epub3.images
    gutenberg_id: XXXXX
    note: "Brief description or why this book is included"
```

IMPORTANT INSTRUCTIONS:
1. Only include books that are in the PUBLIC DOMAIN (typically published before 1928,
   or authors who died 70+ years ago)
2. For each book, search Project Gutenberg (gutenberg.org) to find the correct
   Gutenberg ID number. The URL format is: https://www.gutenberg.org/ebooks/ID.epub3.images
3. CRITICAL: Verify the book has an EPUB format available! Many Gutenberg entries are
   AUDIOBOOKS ONLY (audio/mpeg, audio/ogg). Check via:
   https://gutendex.com/books/ID/ - look for "application/epub+zip" in formats
   If no EPUB exists, use gutenberg_id: 0 and url: "" (the script will skip it)
4. If a book is not on Project Gutenberg at all, use gutenberg_id: 0 and leave url
   empty - the script will try Internet Archive as fallback
5. The filename should be: Title_Author.epub (use underscores, no spaces, ASCII only)
6. Include 15-25 books per category
7. Verify each Gutenberg ID is correct by checking the actual page exists
8. For non-English books, make sure the Gutenberg entry is in the correct language
   (many classic books exist in multiple translations with different IDs)

Sources to verify books:
- Project Gutenberg: https://www.gutenberg.org
- Gutendex API: https://gutendex.com/books/?search=TITLE+AUTHOR
  - Check specific ID: https://gutendex.com/books/ID/
  - Filter by language: https://gutendex.com/books/?languages=de (de, es, fr, en, etc.)
- Internet Archive: https://archive.org (fallback source)
- Standard Ebooks: https://standardebooks.org (high-quality English editions)

GENERATE A YAML FILE FOR THE FOLLOWING TOPIC:
[YOUR TOPIC HERE - e.g., "FRENCH AUTHORS FROM 19TH CENTURY" or "ADVENTURE BOOKS FOR BOYS AGE 9-11"]

--- END PROMPT ---

================================================================================
"""

import os
import sys
import glob
import time
import json
import re
import csv
import urllib.request
import urllib.parse
import ssl
import zipfile
from html.parser import HTMLParser
from datetime import datetime

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

# Optional dependencies
try:
    import pyphen
    PYPHEN_AVAILABLE = True
except ImportError:
    PYPHEN_AVAILABLE = False

try:
    import textstat
    TEXTSTAT_AVAILABLE = True
except ImportError:
    TEXTSTAT_AVAILABLE = False

# Disable SSL verification (needed on some systems)
ssl._create_default_https_context = ssl._create_unverified_context

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = 'Mozilla/5.0 (compatible; EbookManager/1.0)'


# =============================================================================
# Theme Keywords and Labels
# =============================================================================

THEME_KEYWORDS = {
    'en': {
        'coming_of_age': ['growing up', 'childhood', 'youth', 'adolescent', 'mature', 'coming of age', 'rite of passage', 'innocence', 'adulthood'],
        'self_discovery': ['identity', 'self', 'soul', 'purpose', 'meaning', 'destiny', 'truth', 'enlightenment', 'awakening', 'realization'],
        'love_romance': ['love', 'heart', 'passion', 'beloved', 'marriage', 'romance', 'affection', 'devotion', 'desire'],
        'morality': ['moral', 'virtue', 'sin', 'conscience', 'duty', 'honor', 'righteous', 'ethical', 'good', 'evil', 'temptation'],
        'social_criticism': ['society', 'class', 'poverty', 'wealth', 'injustice', 'oppression', 'inequality', 'corruption', 'hypocrisy'],
        'adventure': ['adventure', 'journey', 'quest', 'explore', 'discover', 'voyage', 'expedition', 'danger', 'brave', 'hero'],
        'nature': ['nature', 'forest', 'mountain', 'sea', 'river', 'wild', 'animal', 'natural', 'landscape'],
        'death_mortality': ['death', 'die', 'grave', 'mortal', 'funeral', 'ghost', 'afterlife', 'eternal', 'fate'],
        'family': ['family', 'father', 'mother', 'brother', 'sister', 'parent', 'child', 'home', 'heritage'],
        'war_conflict': ['war', 'battle', 'soldier', 'army', 'enemy', 'fight', 'conflict', 'peace', 'victory', 'defeat'],
        'freedom': ['freedom', 'liberty', 'free', 'escape', 'prison', 'captive', 'chains', 'independence'],
        'faith_religion': ['god', 'faith', 'prayer', 'church', 'soul', 'heaven', 'divine', 'spirit', 'holy', 'salvation'],
        'ambition': ['ambition', 'power', 'success', 'glory', 'fame', 'fortune', 'aspiration', 'dream', 'goal'],
        'isolation': ['alone', 'lonely', 'solitude', 'isolation', 'exile', 'outcast', 'stranger', 'alienation'],
    },
    'de': {
        'coming_of_age': ['erwachsen', 'jugend', 'kindheit', 'reife', 'entwicklung', 'bildung', 'lehrjahre'],
        'self_discovery': ['selbst', 'seele', 'identität', 'sinn', 'wahrheit', 'erkenntnis', 'erwachen', 'bestimmung'],
        'love_romance': ['liebe', 'herz', 'leidenschaft', 'ehe', 'hochzeit', 'zuneigung', 'sehnsucht', 'verlangen'],
        'morality': ['moral', 'tugend', 'sünde', 'gewissen', 'pflicht', 'ehre', 'gut', 'böse', 'schuld'],
        'social_criticism': ['gesellschaft', 'klasse', 'armut', 'reichtum', 'ungerechtigkeit', 'unterdrückung', 'bürger'],
        'adventure': ['abenteuer', 'reise', 'fahrt', 'entdeckung', 'gefahr', 'held', 'mut', 'wagnis'],
        'nature': ['natur', 'wald', 'berg', 'meer', 'fluss', 'wild', 'tier', 'landschaft'],
        'death_mortality': ['tod', 'sterben', 'grab', 'sterblich', 'geist', 'ewigkeit', 'schicksal', 'ende'],
        'family': ['familie', 'vater', 'mutter', 'bruder', 'schwester', 'eltern', 'kind', 'heim', 'erbe'],
        'war_conflict': ['krieg', 'kampf', 'soldat', 'heer', 'feind', 'schlacht', 'frieden', 'sieg', 'niederlage'],
        'freedom': ['freiheit', 'frei', 'flucht', 'gefängnis', 'ketten', 'befreiung', 'unabhängigkeit'],
        'faith_religion': ['gott', 'glaube', 'gebet', 'kirche', 'seele', 'himmel', 'heilig', 'erlösung', 'segen'],
        'ambition': ['ehrgeiz', 'macht', 'erfolg', 'ruhm', 'traum', 'ziel', 'streben', 'aufstieg'],
        'isolation': ['einsamkeit', 'allein', 'einsam', 'fremd', 'außenseiter', 'verbannt', 'verlassen'],
    },
    'es': {
        'coming_of_age': ['crecer', 'juventud', 'infancia', 'madurez', 'adolescencia', 'formación'],
        'self_discovery': ['identidad', 'alma', 'sentido', 'verdad', 'destino', 'despertar', 'iluminación'],
        'love_romance': ['amor', 'corazón', 'pasión', 'matrimonio', 'boda', 'deseo', 'cariño', 'enamorado'],
        'morality': ['moral', 'virtud', 'pecado', 'conciencia', 'deber', 'honor', 'bien', 'mal', 'culpa'],
        'social_criticism': ['sociedad', 'clase', 'pobreza', 'riqueza', 'injusticia', 'opresión', 'corrupción'],
        'adventure': ['aventura', 'viaje', 'búsqueda', 'explorar', 'peligro', 'héroe', 'valiente'],
        'nature': ['naturaleza', 'bosque', 'montaña', 'mar', 'río', 'salvaje', 'animal', 'paisaje'],
        'death_mortality': ['muerte', 'morir', 'tumba', 'mortal', 'fantasma', 'eternidad', 'destino'],
        'family': ['familia', 'padre', 'madre', 'hermano', 'hermana', 'hijo', 'hogar', 'herencia'],
        'war_conflict': ['guerra', 'batalla', 'soldado', 'ejército', 'enemigo', 'lucha', 'paz', 'victoria'],
        'freedom': ['libertad', 'libre', 'escape', 'prisión', 'cadenas', 'independencia', 'liberación'],
        'faith_religion': ['dios', 'fe', 'oración', 'iglesia', 'alma', 'cielo', 'sagrado', 'salvación'],
        'ambition': ['ambición', 'poder', 'éxito', 'gloria', 'fama', 'sueño', 'meta', 'fortuna'],
        'isolation': ['soledad', 'solo', 'solitario', 'aislamiento', 'exilio', 'extranjero', 'abandonado'],
    },
    'fr': {
        'coming_of_age': ['grandir', 'jeunesse', 'enfance', 'maturité', 'adolescence', 'formation'],
        'self_discovery': ['identité', 'âme', 'sens', 'vérité', 'destin', 'éveil', 'illumination'],
        'love_romance': ['amour', 'coeur', 'passion', 'mariage', 'noces', 'désir', 'tendresse', 'amoureux'],
        'morality': ['moral', 'vertu', 'péché', 'conscience', 'devoir', 'honneur', 'bien', 'mal', 'culpabilité'],
        'social_criticism': ['société', 'classe', 'pauvreté', 'richesse', 'injustice', 'oppression', 'corruption'],
        'adventure': ['aventure', 'voyage', 'quête', 'explorer', 'danger', 'héros', 'brave'],
        'nature': ['nature', 'forêt', 'montagne', 'mer', 'rivière', 'sauvage', 'animal', 'paysage'],
        'death_mortality': ['mort', 'mourir', 'tombe', 'mortel', 'fantôme', 'éternité', 'destin'],
        'family': ['famille', 'père', 'mère', 'frère', 'soeur', 'enfant', 'foyer', 'héritage'],
        'war_conflict': ['guerre', 'bataille', 'soldat', 'armée', 'ennemi', 'lutte', 'paix', 'victoire'],
        'freedom': ['liberté', 'libre', 'évasion', 'prison', 'chaînes', 'indépendance', 'libération'],
        'faith_religion': ['dieu', 'foi', 'prière', 'église', 'âme', 'ciel', 'sacré', 'salut'],
        'ambition': ['ambition', 'pouvoir', 'succès', 'gloire', 'renommée', 'rêve', 'but', 'fortune'],
        'isolation': ['solitude', 'seul', 'solitaire', 'isolement', 'exil', 'étranger', 'abandonné'],
    }
}

THEME_LABELS = {
    'coming_of_age': 'Coming of Age',
    'self_discovery': 'Self-Discovery',
    'love_romance': 'Love & Romance',
    'morality': 'Morality & Ethics',
    'social_criticism': 'Social Criticism',
    'adventure': 'Adventure',
    'nature': 'Nature',
    'death_mortality': 'Death & Mortality',
    'family': 'Family',
    'war_conflict': 'War & Conflict',
    'freedom': 'Freedom',
    'faith_religion': 'Faith & Religion',
    'ambition': 'Ambition & Power',
    'isolation': 'Isolation & Alienation',
}


# =============================================================================
# Download Functions
# =============================================================================

def try_download_url(url: str, filepath: str, timeout: int = 60, min_size: int = 10000) -> bool:
    """
    Try to download from a URL. Returns True if successful.
    Validates that the file is a real EPUB (ZIP format) and meets minimum size.
    """
    try:
        request = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = response.read()

            # Check if it's a valid ZIP (EPUB is a ZIP file)
            if not data.startswith(b'PK'):
                return False

            # Check minimum size
            if len(data) < min_size:
                return False

            with open(filepath, 'wb') as f:
                f.write(data)
        return True
    except Exception:
        if os.path.exists(filepath):
            os.remove(filepath)
        return False


def search_internet_archive(title: str, author: str) -> list:
    """
    Search Internet Archive for an EPUB. Returns list of download URLs to try.
    """
    urls = []
    query = f"{title} {author}"
    search_url = f"https://archive.org/advancedsearch.php?q={urllib.parse.quote(query)}+AND+format:EPUB&fl=identifier&output=json&rows=5"

    try:
        request = urllib.request.Request(search_url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(request, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))

            docs = data.get('response', {}).get('docs', [])
            for doc in docs[:3]:
                identifier = doc['identifier']
                metadata_url = f"https://archive.org/metadata/{identifier}"
                try:
                    request = urllib.request.Request(metadata_url, headers={'User-Agent': USER_AGENT})
                    with urllib.request.urlopen(request, timeout=15) as response:
                        metadata = json.loads(response.read().decode('utf-8'))
                        files = metadata.get('files', [])

                        epub_files = []
                        for f in files:
                            name = f.get('name', '')
                            if name.endswith('.epub'):
                                epub_files.append(name)

                        # Prefer _lcp.epub files (usually accessible)
                        epub_files.sort(key=lambda x: (0 if '_lcp.epub' in x else 1, x))

                        for name in epub_files:
                            urls.append(f"https://archive.org/download/{identifier}/{name}")
                except Exception:
                    pass
    except Exception:
        pass

    return urls


def download_book(book: dict, output_dir: str) -> dict:
    """Download a single book. Returns status dict."""
    filepath = os.path.join(output_dir, book['filename'])
    result = {
        'downloaded': False,
        'exists': False,
        'source': None,
        'size_kb': 0,
        'error': None
    }

    # Skip if already exists
    if os.path.exists(filepath):
        result['exists'] = True
        result['size_kb'] = os.path.getsize(filepath) / 1024
        return result

    title = book['title']
    author = book['author']
    primary_url = book.get('url', '')
    gutenberg_id = book.get('gutenberg_id', 0)

    # If no URL and gutenberg_id is 0, book is marked as unavailable - skip download
    if not primary_url and gutenberg_id == 0:
        result['error'] = 'No source available (marked as unavailable)'
        return result

    # Try primary URL (Gutenberg or direct)
    if primary_url and try_download_url(primary_url, filepath):
        result['downloaded'] = True
        result['source'] = 'Gutenberg' if 'gutenberg.org' in primary_url else 'Direct URL'
        result['size_kb'] = os.path.getsize(filepath) / 1024
        return result

    # Only try Internet Archive if book has a valid gutenberg_id (suggests it exists somewhere)
    if gutenberg_id and gutenberg_id > 0:
        ia_urls = search_internet_archive(title, author)
        for ia_url in ia_urls:
            if try_download_url(ia_url, filepath):
                result['downloaded'] = True
                result['source'] = 'Internet Archive'
                result['size_kb'] = os.path.getsize(filepath) / 1024
                return result

    result['error'] = 'Could not download from any source'
    return result


# =============================================================================
# EPUB Metadata Extraction
# =============================================================================

class HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.skip = False
        self.skip_tags = {'script', 'style', 'head', 'meta', 'link'}

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.skip_tags:
            self.skip = True

    def handle_endtag(self, tag):
        if tag.lower() in self.skip_tags:
            self.skip = False

    def handle_data(self, data):
        if not self.skip:
            self.text_parts.append(data)

    def get_text(self):
        return ' '.join(self.text_parts)


def extract_epub_metadata(epub_path: str) -> dict:
    """Extract metadata (title, author) from EPUB file."""
    metadata = {'title': None, 'author': None, 'language': None}

    try:
        with zipfile.ZipFile(epub_path, 'r') as zf:
            # Find OPF file
            opf_path = None
            for name in zf.namelist():
                if name.endswith('.opf'):
                    opf_path = name
                    break

            if not opf_path:
                # Try container.xml
                try:
                    container = zf.read('META-INF/container.xml').decode('utf-8')
                    match = re.search(r'full-path="([^"]+\.opf)"', container)
                    if match:
                        opf_path = match.group(1)
                except Exception:
                    pass

            if opf_path:
                opf_content = zf.read(opf_path).decode('utf-8', errors='ignore')

                # Extract title
                title_match = re.search(r'<dc:title[^>]*>([^<]+)</dc:title>', opf_content, re.IGNORECASE)
                if title_match:
                    metadata['title'] = title_match.group(1).strip()

                # Extract author
                author_match = re.search(r'<dc:creator[^>]*>([^<]+)</dc:creator>', opf_content, re.IGNORECASE)
                if author_match:
                    metadata['author'] = author_match.group(1).strip()

                # Extract language
                lang_match = re.search(r'<dc:language[^>]*>([^<]+)</dc:language>', opf_content, re.IGNORECASE)
                if lang_match:
                    metadata['language'] = lang_match.group(1).strip()[:2].lower()

    except Exception:
        pass

    return metadata


def extract_text_from_epub(epub_path: str, max_chars: int = 500000) -> str:
    """Extract plain text from an EPUB file."""
    text_parts = []
    total_chars = 0

    try:
        with zipfile.ZipFile(epub_path, 'r') as zf:
            html_files = [n for n in zf.namelist()
                         if n.endswith(('.html', '.xhtml', '.htm'))
                         and 'toc' not in n.lower()]

            for name in sorted(html_files):
                if total_chars >= max_chars:
                    break

                try:
                    content = zf.read(name).decode('utf-8', errors='ignore')
                    extractor = HTMLTextExtractor()
                    extractor.feed(content)
                    text = extractor.get_text()
                    text_parts.append(text)
                    total_chars += len(text)
                except Exception:
                    continue

    except Exception:
        return ""

    return ' '.join(text_parts)


def verify_metadata(expected_title: str, expected_author: str, actual_metadata: dict) -> dict:
    """Check if actual metadata matches expected."""
    result = {
        'title_match': False,
        'author_match': False,
        'actual_title': actual_metadata.get('title'),
        'actual_author': actual_metadata.get('author')
    }

    if not actual_metadata.get('title') or not actual_metadata.get('author'):
        return result

    # Normalize for comparison
    def normalize(s):
        if not s:
            return ''
        s = s.lower()
        s = re.sub(r'[^\w\s]', '', s)
        return ' '.join(s.split())

    expected_title_norm = normalize(expected_title)
    expected_author_norm = normalize(expected_author)
    actual_title_norm = normalize(actual_metadata.get('title', ''))
    actual_author_norm = normalize(actual_metadata.get('author', ''))

    # Check title (substring match)
    if expected_title_norm and actual_title_norm:
        # Check if significant words match
        expected_words = set(expected_title_norm.split())
        actual_words = set(actual_title_norm.split())
        common = expected_words & actual_words
        if len(common) >= min(2, len(expected_words)):
            result['title_match'] = True

    # Check author (last name match)
    if expected_author_norm and actual_author_norm:
        expected_parts = expected_author_norm.split()
        actual_parts = actual_author_norm.split()
        # Check if any significant name part matches
        for ep in expected_parts:
            if len(ep) > 2:
                for ap in actual_parts:
                    if len(ap) > 2 and (ep in ap or ap in ep):
                        result['author_match'] = True
                        break

    return result


# =============================================================================
# Reading Difficulty Analysis
# =============================================================================

def detect_language(text: str) -> str:
    """Simple language detection based on common words."""
    text_lower = text.lower()

    lang_markers = {
        'de': ['der', 'die', 'das', 'und', 'ist', 'nicht', 'sie', 'ich', 'ein', 'eine'],
        'es': ['el', 'la', 'los', 'las', 'que', 'de', 'en', 'es', 'por', 'con'],
        'fr': ['le', 'la', 'les', 'des', 'est', 'sont', 'nous', 'vous', 'pas', 'qui'],
        'en': ['the', 'and', 'is', 'are', 'was', 'were', 'have', 'has', 'been', 'will'],
    }

    scores = {}
    for lang, words in lang_markers.items():
        scores[lang] = sum(1 for w in words if f' {w} ' in text_lower)

    return max(scores, key=scores.get)


def count_syllables(word: str, lang: str = 'en') -> int:
    """Count syllables in a word."""
    if PYPHEN_AVAILABLE:
        lang_map = {'en': 'en_US', 'de': 'de_DE', 'es': 'es_ES', 'fr': 'fr_FR'}
        try:
            dic = pyphen.Pyphen(lang=lang_map.get(lang, 'en_US'))
            return len(dic.inserted(word).split('-'))
        except Exception:
            pass

    # Fallback: simple vowel counting
    word = word.lower()
    vowels = 'aeiouyäöüáéíóúàèìòùâêîôûæœ'
    count = 0
    prev_vowel = False
    for char in word:
        is_vowel = char in vowels
        if is_vowel and not prev_vowel:
            count += 1
        prev_vowel = is_vowel
    return max(1, count)


def calculate_reading_difficulty(text: str, lang: str = None) -> dict:
    """Calculate Flesch-Kincaid Grade Level."""
    if not text or len(text) < 100:
        return {'grade_level': None, 'cefr': None, 'error': 'Text too short'}

    if not lang:
        lang = detect_language(text)

    # Use textstat if available
    if TEXTSTAT_AVAILABLE:
        try:
            lang_map = {'en': 'en', 'de': 'de', 'es': 'es', 'fr': 'fr'}
            textstat.set_lang(lang_map.get(lang, 'en'))
            grade = textstat.flesch_kincaid_grade(text)
            return {
                'grade_level': round(grade, 1),
                'cefr': grade_to_cefr(grade),
                'language': lang
            }
        except Exception:
            pass

    # Manual calculation
    text = re.sub(r'[^\w\s.!?]', '', text)
    sentences = max(1, len(re.findall(r'[.!?]+', text)))
    words = text.split()
    word_count = len(words)

    if word_count < 10:
        return {'grade_level': None, 'cefr': None, 'error': 'Not enough words'}

    syllable_count = sum(count_syllables(w, lang) for w in words)
    avg_sentence_length = word_count / sentences
    avg_syllables_per_word = syllable_count / word_count
    grade_level = 0.39 * avg_sentence_length + 11.8 * avg_syllables_per_word - 15.59

    return {
        'grade_level': round(max(0, grade_level), 1),
        'cefr': grade_to_cefr(grade_level),
        'language': lang
    }


def grade_to_cefr(grade_level: float) -> str:
    """Convert Flesch-Kincaid grade to CEFR level."""
    if grade_level is None:
        return None
    if grade_level <= 4:
        return 'A1'
    elif grade_level <= 6:
        return 'A2'
    elif grade_level <= 8:
        return 'B1'
    elif grade_level <= 10:
        return 'B2'
    elif grade_level <= 13:
        return 'C1'
    else:
        return 'C2'


# =============================================================================
# Theme Detection
# =============================================================================

def detect_themes(text: str, lang: str = None, top_n: int = 5) -> list:
    """Detect themes in text based on keyword frequency."""
    if not text:
        return []

    if not lang:
        lang = detect_language(text)

    keywords = THEME_KEYWORDS.get(lang, THEME_KEYWORDS['en'])
    text_lower = text.lower()
    word_count = len(text.split())

    if word_count == 0:
        return []

    theme_scores = {}
    for theme_id, theme_keywords in keywords.items():
        score = 0
        for keyword in theme_keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            matches = len(re.findall(pattern, text_lower, re.IGNORECASE))
            score += matches

        if score > 0:
            # Normalize per 10000 words
            theme_scores[theme_id] = (score / word_count) * 10000

    sorted_themes = sorted(theme_scores.items(), key=lambda x: x[1], reverse=True)

    results = []
    for theme_id, score in sorted_themes[:top_n]:
        if score >= 1:
            results.append(THEME_LABELS.get(theme_id, theme_id))

    return results


# =============================================================================
# Cache Management
# =============================================================================

def load_existing_csv(csv_path: str) -> dict:
    """Load existing CSV and return dict of filename -> row data."""
    cache = {}
    if not os.path.exists(csv_path):
        return cache

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                filename = row.get('filename', '')
                if filename:
                    cache[filename] = row
    except Exception:
        pass

    return cache


def is_book_complete(cached_row: dict) -> bool:
    """Check if a book has been fully processed (downloaded, verified, analyzed)."""
    if not cached_row:
        return False

    # Must have file
    if cached_row.get('file_exists') != 'True':
        return False

    # Must have analysis
    if not cached_row.get('grade_level') or cached_row.get('grade_level') == '':
        return False

    # Must have themes (at least checked)
    # theme_1 can be empty if no themes detected, but we mark as complete
    # We consider it complete if grade_level is set

    return True


# =============================================================================
# Main Processing
# =============================================================================

def find_yaml_files() -> list:
    """Find all YAML config files in the script directory."""
    yaml_files = glob.glob(os.path.join(SCRIPT_DIR, '*.yaml'))
    # Filter out any that don't look like book configs
    valid_files = []
    for f in yaml_files:
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                config = yaml.safe_load(fp)
                if config and 'books' in config and isinstance(config['books'], list):
                    valid_files.append(f)
        except Exception:
            pass
    return sorted(valid_files, key=lambda x: os.path.basename(x))


def process_category(yaml_path: str, force: bool = False) -> tuple:
    """
    Process a single category: download, verify, analyze.
    Returns (results_list, csv_path).
    Only processes books that haven't been completed (unless force=True).
    """
    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    category = config.get('category', os.path.basename(yaml_path).replace('.yaml', ''))
    books = config.get('books', [])

    # Folder name from YAML filename (without extension)
    yaml_basename = os.path.basename(yaml_path)
    folder_name = yaml_basename.replace('.yaml', '')
    folder_path = os.path.join(SCRIPT_DIR, folder_name)

    # CSV path in subfolder
    csv_path = os.path.join(folder_path, 'catalog.csv')

    print(f"\n{'='*60}")
    print(f"Category: {category}")
    print(f"Folder: {folder_name}/")
    print(f"Books: {len(books)}")
    print(f"{'='*60}")

    # Create folder if needed
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"  Created folder: {folder_name}/")

    # Load existing cache
    cache = {} if force else load_existing_csv(csv_path)
    if cache:
        completed = sum(1 for fn in cache if is_book_complete(cache.get(fn)))
        print(f"  Cached: {completed}/{len(books)} books already complete")

    results = []
    skipped = 0
    processed = 0

    for i, book in enumerate(books, 1):
        filename = book['filename']

        # Check if already complete
        if not force and is_book_complete(cache.get(filename)):
            # Use cached result
            row = cache[filename]
            result = {
                'category': row.get('category', category),
                'folder': row.get('folder', folder_name),
                'filename': filename,
                'expected_title': row.get('expected_title', book['title']),
                'expected_author': row.get('expected_author', book['author']),
                'gutenberg_id': row.get('gutenberg_id', book.get('gutenberg_id', '')),
                'note': row.get('note', book.get('note', '')),
                'file_exists': row.get('file_exists') == 'True',
                'file_size_kb': float(row.get('file_size_kb', 0)),
                'download_source': row.get('download_source', ''),
                'download_error': row.get('download_error', ''),
                'actual_title': row.get('actual_title', ''),
                'actual_author': row.get('actual_author', ''),
                'title_match': row.get('title_match') == 'True',
                'author_match': row.get('author_match') == 'True',
                'language': row.get('language', ''),
                'grade_level': float(row['grade_level']) if row.get('grade_level') else None,
                'cefr': row.get('cefr', ''),
                'themes': [row.get(f'theme_{j}', '') for j in range(1, 6) if row.get(f'theme_{j}')],
            }
            results.append(result)
            skipped += 1
            continue

        processed += 1
        print(f"\n  [{i}/{len(books)}] {book['title']}")
        print(f"      Author: {book['author']}")

        result = {
            'category': category,
            'folder': folder_name,
            'filename': filename,
            'expected_title': book['title'],
            'expected_author': book['author'],
            'gutenberg_id': book.get('gutenberg_id', ''),
            'note': book.get('note', ''),
            'file_exists': False,
            'file_size_kb': 0,
            'download_source': None,
            'download_error': None,
            'actual_title': None,
            'actual_author': None,
            'title_match': None,
            'author_match': None,
            'language': None,
            'grade_level': None,
            'cefr': None,
            'themes': [],
        }

        filepath = os.path.join(folder_path, filename)

        # Step 1: Download if needed
        dl_result = download_book(book, folder_path)
        if dl_result['downloaded']:
            print(f"      Downloaded from {dl_result['source']} ({dl_result['size_kb']:.1f} KB)")
            result['download_source'] = dl_result['source']
        elif dl_result['exists']:
            print(f"      Already exists ({dl_result['size_kb']:.1f} KB)")
        elif dl_result['error']:
            print(f"      ERROR: {dl_result['error']}")
            result['download_error'] = dl_result['error']
        time.sleep(0.3)  # Be nice to servers

        # Step 2: Check file exists
        if os.path.exists(filepath):
            result['file_exists'] = True
            result['file_size_kb'] = round(os.path.getsize(filepath) / 1024, 1)

            # Step 3: Extract and verify metadata
            epub_meta = extract_epub_metadata(filepath)
            result['actual_title'] = epub_meta.get('title')
            result['actual_author'] = epub_meta.get('author')

            verify = verify_metadata(book['title'], book['author'], epub_meta)
            result['title_match'] = verify['title_match']
            result['author_match'] = verify['author_match']

            if verify['title_match'] and verify['author_match']:
                print(f"      Metadata: OK")
            else:
                print(f"      Metadata: MISMATCH")
                if not verify['title_match']:
                    print(f"        Expected: {book['title']}")
                    print(f"        Actual: {epub_meta.get('title', 'Unknown')}")
                if not verify['author_match']:
                    print(f"        Expected: {book['author']}")
                    print(f"        Actual: {epub_meta.get('author', 'Unknown')}")

            # Step 4: Extract text and analyze
            print(f"      Analyzing...")
            text = extract_text_from_epub(filepath)

            if text and len(text) > 500:
                # Detect language
                lang = epub_meta.get('language') or detect_language(text)
                result['language'] = lang

                # Calculate difficulty
                difficulty = calculate_reading_difficulty(text, lang)
                result['grade_level'] = difficulty.get('grade_level')
                result['cefr'] = difficulty.get('cefr')

                # Detect themes
                themes = detect_themes(text, lang, top_n=5)
                result['themes'] = themes

                print(f"      Language: {lang.upper()}")
                print(f"      Difficulty: Grade {result['grade_level']} ({result['cefr']})")
                print(f"      Themes: {', '.join(themes[:3]) if themes else 'None detected'}")
            else:
                print(f"      Could not extract text for analysis")
        else:
            print(f"      File not found: {filename}")

        results.append(result)

    if skipped > 0:
        print(f"\n  Skipped {skipped} already-complete books")
    if processed > 0:
        print(f"  Processed {processed} books")

    return results, csv_path


def write_csv(results: list, output_path: str):
    """Write results to CSV file."""
    if not results:
        print("No results to write")
        return

    fieldnames = [
        'category', 'folder', 'filename',
        'expected_title', 'expected_author', 'gutenberg_id', 'note',
        'file_exists', 'file_size_kb', 'download_source', 'download_error',
        'actual_title', 'actual_author', 'title_match', 'author_match',
        'language', 'grade_level', 'cefr',
        'theme_1', 'theme_2', 'theme_3', 'theme_4', 'theme_5'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in results:
            row = {k: r.get(k, '') for k in fieldnames if not k.startswith('theme_')}
            themes = r.get('themes', [])
            for i in range(5):
                row[f'theme_{i+1}'] = themes[i] if i < len(themes) else ''
            writer.writerow(row)

    print(f"  CSV written: {os.path.basename(output_path)}")


def main():
    # Parse arguments
    force = '--force' in sys.argv

    # Get category argument (if any)
    category_arg = None
    for arg in sys.argv[1:]:
        if not arg.startswith('--'):
            category_arg = arg
            break

    print("""
================================================================================
                         EBOOK MANAGER
    Download, Verify, Analyze, and Catalog your EPUB library
================================================================================
    """)

    if not PYPHEN_AVAILABLE:
        print("Note: pyphen not installed - syllable counting will be approximate")
        print("      Install with: pip install pyphen")
    if not TEXTSTAT_AVAILABLE:
        print("Note: textstat not installed - using manual calculation")
        print("      Install with: pip install textstat")

    if force:
        print("\nMode: FORCE (re-analyze all books)")
    else:
        print("\nMode: INCREMENTAL (skip already-complete books)")

    # Find YAML files
    if category_arg:
        if not category_arg.endswith('.yaml'):
            category_arg += '.yaml'
        yaml_path = os.path.join(SCRIPT_DIR, category_arg)
        if not os.path.exists(yaml_path):
            print(f"\nERROR: Config file not found: {yaml_path}")
            print("\nAvailable YAML files:")
            for f in find_yaml_files():
                print(f"  - {os.path.basename(f).replace('.yaml', '')}")
            sys.exit(1)
        yaml_files = [yaml_path]
    else:
        yaml_files = find_yaml_files()

    if not yaml_files:
        print("\nERROR: No valid YAML config files found!")
        print(f"Looking in: {SCRIPT_DIR}")
        print("\nA valid YAML file must contain 'books' as a list.")
        print("See the LLM prompt at the top of this script for the format.")
        sys.exit(1)

    print(f"\nFound {len(yaml_files)} YAML config file(s):")
    for f in yaml_files:
        print(f"  - {os.path.basename(f)}")

    # Process all categories
    total_books = 0
    total_exists = 0
    total_verified = 0
    total_analyzed = 0

    for yaml_path in yaml_files:
        results, csv_path = process_category(yaml_path, force)

        # Write CSV to subfolder
        write_csv(results, csv_path)

        # Update totals
        total_books += len(results)
        total_exists += sum(1 for r in results if r.get('file_exists'))
        total_verified += sum(1 for r in results if r.get('title_match') and r.get('author_match'))
        total_analyzed += sum(1 for r in results if r.get('grade_level') is not None)

    # Summary
    print(f"""
================================================================================
                              COMPLETE
================================================================================
  Total books:      {total_books}
  Files present:    {total_exists}/{total_books}
  Metadata OK:      {total_verified}/{total_exists}
  Analyzed:         {total_analyzed}/{total_exists}
  CSV files:        Written to each category subfolder as 'catalog.csv'
================================================================================
    """)


if __name__ == "__main__":
    main()
