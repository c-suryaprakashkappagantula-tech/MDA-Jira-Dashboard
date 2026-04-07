# MDA Jira Dashboard V2.0

Self-contained Streamlit dashboard that automates QMetry/Jira data extraction, pivot generation, and weekly status PPTX report building.

## Modules

1. **QMetry Extract + Pivot (Labels)** — Exports test cases from QMetry, filters by assignee, creates labels pivot
2. **Jira Defect Extract + Pivot** — Searches Jira defects via JQL + REST API, filters by reporter, creates status/priority pivot
3. **Test Execution Report** — Fetches QMetry test execution summary, exports CSV, generates styled Excel
4. **PPT Generation** — Auto-builds weekly status deck when all 3 modules complete

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

Copy `.env.example` to `.env` and fill in your Jira credentials.

## Run

```bash
streamlit run MDA_Jira_Dashboard_V2.0.py
```

## Template

The `templates/` folder contains the Slide 2 (Delivery Updates) PPTX template used during deck generation.
