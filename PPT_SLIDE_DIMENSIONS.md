# MDA Jira Dashboard — PPT Slide Dimensions & Layout Spec

## Global Dimensions (ALL slides)

| Property | Value |
|----------|-------|
| Slide Width | 13.333" (widescreen 16:9) |
| Slide Height | 7.50" |
| Format | Widescreen |
| Row height locking | None (PowerPoint auto-expands rows to fit content) |

## Slide 2: Delivery Updates (Template-based)

**Template file:** `MDA-Jira-Dashboard/templates/MDA QA INTG_Slide2.pptx`

The template is cloned as-is (no rescaling). Data is populated into named table shapes.

| Element | Position | Size |
|---------|----------|------|
| Title "MDA QA INTG: Delivery Updates" | left=2.85", top=0.05" | w=6.37", h=1.90" |
| "Key Highlights" banner | left=0.81", top=1.32" | w=3.14", h=0.33" |
| Key Highlights text box (Rectangle 2) | left=0.80", top=1.70" | w=4.10", h=2.53" |
| "Execution Highlights" banner | left=6.06", top=0.64" | w=3.22", h=0.32" |
| Manual date textbox (TextBox 23) | left=6.12", top=1.32" | w=2.69", h=0.25" |
| Table 1 (Manual execution) | left=6.18", top=1.59" | w=5.91", h=1.40" |
| "Automation" label (TextBox 24) | left=6.10", top=3.24" | w=1.84", h=0.25" |
| Table 25 (Automation) | left=6.21", top=3.49" | w=5.79", h=0.82" |
| "Defect Summary" label (TextBox 26) | left=6.10", top=4.39" | w=1.84", h=0.25" |
| Table 4 (Defect Summary) | left=6.19", top=4.68" | w=5.79", h=1.34" |

### Table 1 Column Headers & Widths
| Header | Width |
|--------|-------|
| Release# | 1.73" |
| Total | 0.46" |
| Passed | 0.73" |
| Failed | 0.55" |
| In Progress | 0.79" |
| No Run | 0.45" |
| NA | 0.45" |
| Blocked | 0.75" |

### Table 25 (Automation) Column Headers & Widths
| Header | Width |
|--------|-------|
| Release# | 1.47" |
| Executed | 0.79" |
| Passed | 0.99" |
| Failed | 1.27" |
| In Prog | 1.27" |

### Table 4 (Defect Summary) Column Headers & Widths
| Header | Width |
|--------|-------|
| Releases | 1.45" |
| P0 | 0.58" |
| P1 | 0.77" |
| P2 | 0.57" |
| P3 | 0.81" |
| P4 | 0.81" |
| Total | 0.79" |

## Slides 3+ : Data Tables (Generated)

| Property | Value |
|----------|-------|
| Side margin | 0.90" each side |
| Usable table width | 11.53" (13.333 - 1.80) |
| Title top | 0.65" |
| Table top | 0.92" |
| Header row height | 0.45" (allows 2-line word wrap) |
| Data row height | 0.22" |
| Header font | Segoe UI, 10pt, Bold, White on NAVY (#0B1D39) |
| Header word_wrap | True (wraps at word boundaries, never mid-word) |
| Body font | Segoe UI, 8pt |
| Cell autofit | normAutofit fontScale=50000 (data cells only) |
| Header autofit | None (relies on word wrap + taller row) |
| Cell margins | left=0.02", right=0.02", top=0, bottom=0 |
| Word wrap | False |
| Max rows per slide | ~25 (calculated from available height) |
| Bottom margin | 2cm from slide bottom |

### Header Abbreviations
| Original | Abbreviated |
|----------|-------------|
| Not Applicable | NA |
| Blocker/Emergency | Block/Emrg |
| Critical/High | Crit/High |
| Major/Medium | Major/Med |
| Minor/Low | Minor/Low |
| Work In Progress | WIP |
| Not Executed | No Run |
| Conditional Pass | Cond Pass |
| Execution Assignee | Assignee |
| P0 Blocker/Emergency | P0 |
| P1 Critical/High | P1 |
| P2 Major/Medium | P2 |
| P3 Minor/Low | P3 |

## Formatting Rules

1. **Font**: Segoe UI everywhere (no Arial, no exceptions)
2. **Colors**: NAVY (#0B1D39) header background + WHITE text on all generated table slides
3. **Slide 2**: Uses template colors (steel blue) — not overridden
4. **No text truncation**: Rely on `normAutofit` (shrink-to-fit at 50%) instead
5. **No word wrap**: All cells have word_wrap=False
6. **Column widths**: Proportional based on content length, always sum to usable width
7. **"TOSCA" removed**: Not in title slide or output filename
8. **Output filename**: `MDA_QA_TMobile_INTG_Weekly_Status_<YYYYMMDD_HHMMSS>.pptx`
