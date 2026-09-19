import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber

VECTOR_DIMENSION = 128
MAX_CHUNK_CHARS = 900
OVERLAP_CHARS = 140

STOPWORDS = {
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'how', 'i', 'if', 'in', 'is', 'it', 'its',
    'of', 'on', 'or', 'our', 'that', 'the', 'their', 'them', 'this', 'to', 'was', 'what', 'when', 'where',
    'which', 'who', 'why', 'with', 'you', 'your', 'can', 'could', 'do', 'does', 'did', 'have', 'has', 'had',
    'me', 'my', 'we', 'they', 'there', 'here', 'about', 'into', 'over', 'than', 'then', 'show', 'tell', 'please',
    'help', 'more', 'less', 'most', 'least'
}


def normalize_text(text):
    text = re.sub(r'\s+', ' ', str(text or '')).strip()
    return text


def tokenize(text):
    tokens = re.findall(r'[a-z0-9]+', str(text or '').lower())
    return [token for token in tokens if len(token) > 2 and token not in STOPWORDS]


def hash_token(token, dimension=VECTOR_DIMENSION):
    hash_value = 2166136261
    for char in token:
        hash_value ^= ord(char)
        hash_value = (hash_value * 16777619) & 0xFFFFFFFF
    return abs(hash_value) % dimension


def build_embedding(text, dimension=VECTOR_DIMENSION):
    vector = [0.0] * dimension
    tokens = tokenize(text)
    if not tokens:
        return vector

    for token in tokens:
        slot = hash_token(token, dimension)
        vector[slot] += 1.0

    magnitude = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [round(value / magnitude, 6) for value in vector]


def split_into_chunks(text):
    cleaned = normalize_text(text)
    if not cleaned:
        return []

    chunks = []
    buffer = []
    current_length = 0

    for paragraph in re.split(r'\n\s*\n', cleaned):
        paragraph = normalize_text(paragraph)
        if not paragraph:
            continue

        if current_length + len(paragraph) > MAX_CHUNK_CHARS and buffer:
            chunks.append(' '.join(buffer).strip())
            if OVERLAP_CHARS > 0 and chunks[-1]:
                overlap_source = chunks[-1][-OVERLAP_CHARS:]
                buffer = [overlap_source, paragraph]
                current_length = len(overlap_source) + len(paragraph)
            else:
                buffer = [paragraph]
                current_length = len(paragraph)
            continue

        buffer.append(paragraph)
        current_length += len(paragraph) + 1

    if buffer:
        chunks.append(' '.join(buffer).strip())

    return [chunk for chunk in chunks if chunk]


def title_from_filename(path):
    stem = path.stem.replace('_', ' ').replace('-', ' ').strip()
    return stem if stem else path.name


def extract_pdf_chunks(pdf_path):
    entries = []
    title = title_from_filename(pdf_path)

    with pdfplumber.open(pdf_path) as pdf:
        for index, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ''
            if not text.strip():
                continue

            page_chunks = split_into_chunks(text)
            if not page_chunks:
                continue

            for chunk_index, chunk_text in enumerate(page_chunks, start=1):
                entries.append({
                    'source_file': pdf_path.name,
                    'source_name': title,
                    'source_type': 'pdf',
                    'title': title,
                    'page_start': index,
                    'page_end': index,
                    'chunk_index': chunk_index,
                    'text': chunk_text,
                    'tokens': tokenize(chunk_text),
                    'embedding': build_embedding(chunk_text),
                })

    return entries


def extract_text_chunks(text_path):
    title = title_from_filename(text_path)
    text = text_path.read_text(encoding='utf-8', errors='ignore')
    chunks = split_into_chunks(text)
    entries = []

    for chunk_index, chunk_text in enumerate(chunks, start=1):
        entries.append({
            'source_file': text_path.name,
            'source_name': title,
            'source_type': text_path.suffix.lstrip('.').lower(),
            'title': title,
            'page_start': None,
            'page_end': None,
            'chunk_index': chunk_index,
            'text': chunk_text,
            'tokens': tokenize(chunk_text),
            'embedding': build_embedding(chunk_text),
        })

    return entries


def build_index(documentation_dir):
    chunks = []
    for path in sorted(documentation_dir.iterdir()):
        if path.name.startswith('.') or path.is_dir():
            continue

        suffix = path.suffix.lower()
        if suffix == '.pdf':
            chunks.extend(extract_pdf_chunks(path))

    return {
        'dimension': VECTOR_DIMENSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'source_directory': str(documentation_dir),
        'chunk_count': len(chunks),
        'chunks': chunks,
    }


def main():
    if len(sys.argv) < 2:
        print('Usage: build_civora_knowledge_index.py <output-path>', file=sys.stderr)
        return 1

    output_path = Path(sys.argv[1]).resolve()
    backend_dir = Path(__file__).resolve().parents[1]
    documentation_dir = backend_dir.parent / 'documentation'

    if not documentation_dir.exists():
        print(f'Documentation directory not found: {documentation_dir}', file=sys.stderr)
        return 1

    index = build_index(documentation_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'output_path': str(output_path), 'chunk_count': index['chunk_count']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
