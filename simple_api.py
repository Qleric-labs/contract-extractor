"""
Contract Extraction API (Open Source Version)
=============================================

A simplified Flask API server for the Contract Extractor.
This version removes SaaS logic
and serves as a clean reference implementation.

Usage:
    pip install -r requirements.txt
    python simple_api.py
"""

import os
import logging
import base64
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename
import fitz  # PyMuPDF
from dotenv import load_dotenv

from contract_extractor import ContractExtractor, TIER_FIELDS

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
ALLOWED_EXTENSIONS = {"pdf"}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

# Initialize Extractor
try:
    extractor = ContractExtractor()
    logger.info("ContractExtractor initialized successfully.")
except Exception as e:
    logger.error(f"FATAL: Could not initialize ContractExtractor. {e}")
    extractor = None

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "extractor_loaded": extractor is not None,
        "tiers_available": list(TIER_FIELDS.keys())
    })

@app.route("/analyze", methods=["POST"])
def analyze_contract():
    """
    Analyze a contract PDF.
    
    Form Parameters:
        file: The PDF file
        tier: 'essential', 'professional', 'enterprise' (default: essential)
        
    Returns:
        JSON extraction results including highlighting data.
    """
    if not extractor:
        return jsonify({"error": "Extractor not initialized"}), 503

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    tier = request.form.get("tier", "essential")
    
    if tier not in TIER_FIELDS:
        tier = "essential"

    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            logger.info(f"Processing {filename} (Tier: {tier})")
            
            # Read file
            file_content = file.read()
            
            # Extract
            results = extractor.extract_from_pdf(file_content, tier=tier)
            
            if "error" in results:
                return jsonify(results), 500
                
            return jsonify(results)

        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    return jsonify({"error": "Invalid file type"}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
