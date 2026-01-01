# Qleric Contract Extractor (Open Source)

> **Contract extraction engine** - extract 60+ fields from PDF contracts with Claude AI.

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

## Disclaimer

This repository contains a curated, handpicked subset of the full Qleric Contract Extractor solution, made available as open source to comply with licensing requirements (e.g., GPL dependencies like PyMuPDF used in the SaaS version). It is manually synced with the proprietary SaaS application approximately once a month to reflect key updates. For access to the complete, production-ready solution or enterprise features, please refer to the main Qleric platform.

## Features

- **60-field extraction taxonomy** - dates, parties, financial terms, liability, IP, compliance, and more
- **3-tier extraction** - Essential (9 fields), Professional (18 fields), Enterprise (25 fields)
- **Custom field selection** - mix-match any fields from the 60-field bank (max 25 per extraction)
- **Smart chunking** - handles contracts >50 pages with intelligent section boundary detection
- **Table extraction** - payment schedules, fee tables with automatic normalization
- **Grounding verification** - validates extracted values against source text

## Installation

```bash
pip install -r requirements.txt
cp example.env .env
# Edit .env and add your ANTHROPIC_API_KEY
```

## Security

1. Copy `example.env` to `.env` and add your actual API key.
2. **Never commit your `.env` file** — it contains sensitive credentials.
3. Ensure `.env` is listed in your `.gitignore`.

## Quick Start (Python)

```python
from contract_extractor import ContractExtractor

extractor = ContractExtractor()

# Extract from PDF bytes
with open("contract.pdf", "rb") as f:
    result = extractor.extract_from_pdf(f.read(), tier="professional")

print(result["analysis"])
```

## Custom Field Selection

Pick specific fields from the 60-field bank instead of using predefined tiers:

```python
result = extractor.extract_from_pdf(
    pdf_bytes,
    custom_fields=[
        "effective_date",
        "expiration_date", 
        "liability_cap",
        "gdpr_obligations",
        "termination_for_convenience"
    ]
)
```

## API Server (Optional)

Run the included Flask API for HTTP access:

```bash
python simple_api.py
```

Send an extraction request:

```bash
curl -X POST -F "file=@contract.pdf" -F "tier=professional" http://localhost:5000/analyze
```

## Extraction Tiers

| Tier | Fields | Use Case |
|------|--------|----------|
| Essential | 9 | Quick due diligence |
| Professional | 18 | Standard review |
| Enterprise | 25 | Full extraction |
| Custom | 1-25 | Mix-match from 60-field bank |

## Available Fields

| Category | Fields |
|----------|--------|
| **Core** | effective_date, expiration_date, parties |
| **Financial** | total_contract_value, payment_terms, currency |
| **Termination** | termination_notice_period, renewal_terms, governing_law |
| **Liability** | liability_cap, indemnification_clauses, insurance_requirements, limitation_of_liability |
| **Performance** | deliverables, sla_terms, performance_metrics, acceptance_criteria |
| **IP** | ip_ownership, license_scope, usage_restrictions |
| **Compliance** | confidentiality_period, non_compete_terms, arbitration_clause, audit_rights, data_protection |
| **Administrative** | notice_address, amendment_process, assignment_rights, force_majeure |
| **+ Extended** | 30+ additional fields (GDPR, termination types, risk management, commercial terms) |

## Environment Variables

```bash
ANTHROPIC_API_KEY=your_api_key_here

# Optional: Custom extraction prompt
CLAUDE_EXTRACTION_PROMPT=your_custom_prompt
```

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

If you modify this code and provide it as a service, you must make your modifications available under the same license.

Commercial licensing available at [qleric.com](https://qleric.com).

## Credits

Built with:
- [PyMuPDF](https://pymupdf.readthedocs.io/) - PDF processing
- [Anthropic Claude](https://anthropic.com/) - LLM extraction
- [spaCy](https://spacy.io/) - NLP utilities
