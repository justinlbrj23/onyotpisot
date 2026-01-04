name: OCR Tesseract Pipeline

on:
  workflow_dispatch:
  push:
    branches:
      - main

jobs:
  ocr:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Install system dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y poppler-utils ghostscript imagemagick tesseract-ocr

      - name: Show working directory and files
        run: |
          echo "Workspace: ${{ github.workspace }}"
          ls -l ${{ github.workspace }}

      - name: Run OCR on W8-BEN(2).pdf
        run: |
          node ocr_tesseract.js '${{ github.workspace }}/W8-BEN(2).pdf'

      - name: Upload OCR results
        uses: actions/upload-artifact@v4
        with:
          name: ocr-output
          path: ocr_output/