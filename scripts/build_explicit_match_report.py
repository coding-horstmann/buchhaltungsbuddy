from __future__ import annotations

import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from reconcile.evidence import build_document_evidence, build_open_bank_after_evidence  # noqa: E402
from reconcile.matching import MatchSettings, reconcile  # noqa: E402
from reconcile.parsers import parse_accountable_file, parse_bank_file, parse_paypal_file  # noqa: E402
from reconcile.paypal import match_docs_to_paypal, match_paypal_transfers_to_bank  # noqa: E402


OUTPUT_DIR = ROOT / "outputs"
REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_beleg_report.pdf"
CONTROL_REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_kontroll_report.pdf"
HYPOTHESIS_REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_hypothesen_report.pdf"
LEDGER_EXPERIMENT_REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_ledger_experiment_report.pdf"


def main() -> int:
    OUTPUT_DIR.mkdir(exist_ok=True)
    settings = MatchSettings()

    accountable = parse_accountable_file("Daten.xlsx", (ROOT / "Daten.xlsx").read_bytes())
    bank_parsed = parse_bank_file("fyrst.csv", (ROOT / "fyrst.csv").read_bytes())
    paypal_path = ROOT / "paypal.CSV"
    paypal_parsed = parse_paypal_file("paypal.CSV", paypal_path.read_bytes()) if paypal_path.exists() else None

    docs = accountable.frame
    bank = bank_parsed.frame
    paypal = paypal_parsed.frame if paypal_parsed is not None else pd.DataFrame()

    matches, links = reconcile(docs, bank, settings)
    paypal_doc_matches, paypal_doc_links = match_docs_to_paypal(docs, paypal, settings)
    paypal_bank_matches = match_paypal_transfers_to_bank(paypal, bank, settings)
    evidence = build_document_evidence(docs, bank, paypal, matches, links, paypal_doc_matches, paypal_doc_links, paypal_bank_matches)
    open_doc_ids = set(evidence.loc[evidence["evidence_level"] == "offen", "doc_id"])
    open_docs = docs[docs["doc_id"].isin(open_doc_ids)].copy()
    open_bank = build_open_bank_after_evidence(bank, matches, paypal_bank_matches)

    doc_report = build_doc_report(evidence)
    bank_report = build_bank_report(bank, evidence, matches, paypal_bank_matches)

    doc_report.to_csv(OUTPUT_DIR / "explicit_report_belege_mit_umsatzmatch.csv", sep=";", index=False, encoding="utf-8-sig")
    bank_report.to_csv(OUTPUT_DIR / "explicit_report_umsaetze_mit_belegmatch.csv", sep=";", index=False, encoding="utf-8-sig")

    build_pdf(docs, bank, paypal, evidence, open_docs, open_bank, doc_report, bank_report)
    print(REPORT_PATH)
    print(CONTROL_REPORT_PATH)
    print(HYPOTHESIS_REPORT_PATH)
    print(LEDGER_EXPERIMENT_REPORT_PATH)
    return 0


def build_doc_report(evidence: pd.DataFrame, manual_edits: dict[str, dict[str, object]] | pd.DataFrame | None = None) -> pd.DataFrame:
    manual_by_doc = normalize_manual_edits(manual_edits)
    rows: list[dict[str, object]] = []
    for _, row in evidence.sort_values(["doc_type", "date", "doc_ref"]).iterrows():
        platforms = platform_label(row.get("platforms") or row.get("platform"))
        fyrst_parts = []
        if text(row.get("fyrst_dates")) or text(row.get("fyrst_amounts")):
            fyrst_parts.append(join_nonempty([row.get("fyrst_dates"), row.get("fyrst_amounts"), row.get("fyrst_counterparties")]))
        if text(row.get("bridge_fyrst_dates")) or text(row.get("bridge_fyrst_amounts")):
            fyrst_parts.append("PayPal->FYRST: " + join_nonempty([row.get("bridge_fyrst_dates"), row.get("bridge_fyrst_amounts")]))
        if text(row.get("platform_bridge_fyrst_dates")) or text(row.get("platform_bridge_fyrst_amounts")):
            label = f"{platforms}->FYRST" if platforms else "Plattform->FYRST"
            fyrst_parts.append(label + ": " + join_nonempty([row.get("platform_bridge_fyrst_dates"), row.get("platform_bridge_fyrst_amounts")]))

        support_parts = []
        if text(row.get("paypal_dates")) or text(row.get("paypal_amounts")):
            support_parts.append("PayPal: " + join_nonempty([row.get("paypal_dates"), row.get("paypal_amounts"), row.get("paypal_counterparties")]))
        if text(row.get("platform_dates")) or text(row.get("platform_amounts")):
            support_parts.append((platforms or "Plattform") + ": " + join_nonempty([row.get("platform_dates"), row.get("platform_amounts"), row.get("platform_order_ids"), row.get("platform_counterparties")]))

        report_row = {
            "Beleg": row.get("doc_ref", ""),
            "Typ": "Einnahme" if row.get("doc_type") == "income" else "Ausgabe",
            "Belegdatum": row.get("date"),
            "Datum-Basis": row.get("date_source", ""),
            "Betrag": row.get("amount"),
            "Plattform": row.get("platform", ""),
            "Gegenpartei": row.get("counterparty", ""),
            "Beschreibung": row.get("description", ""),
            "Status": row.get("evidence_level", ""),
            "FYRST-Umsatzmatch": " | ".join(part for part in fyrst_parts if part),
            "Detailspur": " | ".join(part for part in support_parts if part),
            "Matchlogik": row.get("evidence_path", ""),
            "Hinweis": row.get("evidence_note", ""),
            "Manuell geaendert": "",
        }
        apply_manual_doc_edit(report_row, row, manual_by_doc)
        rows.append(report_row)
    return pd.DataFrame(rows)


def build_bank_report(
    bank: pd.DataFrame,
    evidence: pd.DataFrame,
    matches: pd.DataFrame,
    paypal_bank_matches: pd.DataFrame,
    platform_bank_matches: pd.DataFrame | None = None,
) -> pd.DataFrame:
    by_tx: dict[str, list[pd.Series]] = defaultdict(list)
    for _, row in evidence.iterrows():
        tx_ids = split_ids(row.get("fyrst_tx_ids")) + split_ids(row.get("bridge_fyrst_tx_ids")) + split_ids(row.get("platform_bridge_fyrst_tx_ids"))
        for tx_id in tx_ids:
            by_tx[tx_id].append(row)

    direct_tx_ids = set(matches["tx_id"]) if not matches.empty else set()
    paypal_bridge_tx_ids = set(paypal_bank_matches["tx_id"]) if not paypal_bank_matches.empty else set()
    platform_bridge_tx_ids = set(platform_bank_matches["tx_id"]) if platform_bank_matches is not None and not platform_bank_matches.empty else set()
    explained_tx_ids = direct_tx_ids | paypal_bridge_tx_ids | platform_bridge_tx_ids

    rows: list[dict[str, object]] = []
    for _, tx in bank.sort_values(["date", "amount"]).iterrows():
        tx_id = str(tx.get("tx_id"))
        linked = by_tx.get(tx_id, [])
        if linked or tx_id in explained_tx_ids:
            status = "gematcht"
        else:
            status = "offen"
        fallback_logic = ""
        if not linked and tx_id in paypal_bridge_tx_ids:
            fallback_logic = "PayPal-Transfer zu FYRST"
        elif not linked and tx_id in platform_bridge_tx_ids:
            fallback_logic = "Plattform-Auszahlung zu FYRST"
        elif not linked and tx_id in direct_tx_ids:
            fallback_logic = "FYRST-Match ohne Belegdetail in Kontrollliste"
        belegsumme = round(sum(float(row.get("amount") or 0) for row in linked), 2)
        differenz = round(float(tx.get("amount") or 0) - belegsumme, 2)
        rows.append(
            {
                "FYRST-Datum": tx.get("date"),
                "FYRST-Betrag": tx.get("amount"),
                "Belegsumme": belegsumme,
                "Differenz": differenz,
                "Plattform": tx.get("platform", ""),
                "Gegenpartei": tx.get("counterparty", ""),
                "Verwendungszweck": tx.get("description", ""),
                "Status": status,
                "Belegmatches": join_unique(row.get("doc_ref") for row in linked),
                "Belegtypen": join_unique("Einnahme" if row.get("doc_type") == "income" else "Ausgabe" for row in linked),
                "Belegbeträge": join_unique(format_amount(row.get("amount")) for row in linked),
                "Matchlogik": join_unique(row.get("evidence_path") for row in linked) or fallback_logic,
                "Hinweis": join_unique(row.get("evidence_note") for row in linked),
                "tx_id": tx.get("tx_id", ""),
            }
        )
    return pd.DataFrame(rows)


def build_settlement_detail_report(bank_report: pd.DataFrame, evidence: pd.DataFrame) -> pd.DataFrame:
    """Detail view for real marketplace settlement payments and withdrawals."""
    if bank_report.empty:
        return pd.DataFrame()
    bank_by_tx = {str(row.get("tx_id")): row for _, row in bank_report.iterrows()}
    rows: list[dict[str, object]] = []
    seen_tx_ids: set[str] = set()

    for _, doc in evidence.iterrows():
        if not is_marketplace_settlement_doc(doc):
            continue
        add_settlement_rows(rows, seen_tx_ids, bank_by_tx, doc, "Plattform", "platform_bridge_fyrst_tx_ids")
        if "batch" in text(doc.get("fyrst_methods")) and is_marketplace_settlement_text(
            doc.get("fyrst_counterparties"),
            doc.get("fyrst_methods"),
            doc.get("platforms"),
            doc.get("platform"),
        ):
            add_settlement_rows(rows, seen_tx_ids, bank_by_tx, doc, "FYRST-Sammelmatch", "fyrst_tx_ids")

    for _, bank_row in bank_report.iterrows():
        tx_id = str(bank_row.get("tx_id"))
        if tx_id in seen_tx_ids:
            continue
        logic = text(bank_row.get("Matchlogik"))
        if not is_marketplace_settlement_text(
            bank_row.get("Plattform"),
            bank_row.get("Gegenpartei"),
            bank_row.get("Verwendungszweck"),
            logic,
        ):
            continue
        if "paypal" in logic.lower():
            continue
        rows.append(settlement_row(bank_row, "ohne Einzelbeleg", "", "", "", "", "", logic))
        seen_tx_ids.add(tx_id)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["FYRST-Datum", "FYRST-Betrag", "Beleg"]).reset_index(drop=True)


def add_settlement_rows(
    rows: list[dict[str, object]],
    seen_tx_ids: set[str],
    bank_by_tx: dict[str, pd.Series],
    doc: pd.Series,
    source: str,
    tx_column: str,
) -> None:
    tx_ids = split_ids(doc.get(tx_column))
    for tx_id in tx_ids:
        bank_row = bank_by_tx.get(tx_id)
        if bank_row is None:
            continue
        if source == "FYRST-Sammelmatch" and not is_marketplace_settlement_text(
            bank_row.get("Plattform"),
            bank_row.get("FYRST-Gegenpartei"),
            bank_row.get("Verwendungszweck"),
        ):
            continue
        detail = settlement_detail_for_source(doc, source)
        rows.append(
            settlement_row(
                bank_row,
                source,
                doc.get("doc_ref", ""),
                "Einnahme" if doc.get("doc_type") == "income" else "Ausgabe",
                doc.get("amount", ""),
                doc.get("counterparty", ""),
                detail,
                doc.get("evidence_path", ""),
            )
        )
        seen_tx_ids.add(tx_id)


def settlement_row(
    bank_row: pd.Series,
    source: str,
    doc_ref: object,
    doc_type: object,
    doc_amount: object,
    counterparty: object,
    detail: object,
    logic: object,
) -> dict[str, object]:
    return {
        "tx_id": bank_row.get("tx_id", ""),
        "FYRST-Datum": bank_row.get("FYRST-Datum", ""),
        "FYRST-Betrag": bank_row.get("FYRST-Betrag", ""),
        "Belegsumme": bank_row.get("Belegsumme", ""),
        "Rest": bank_row.get("Differenz", ""),
        "Plattform": bank_row.get("Plattform", ""),
        "FYRST-Gegenpartei": bank_row.get("Gegenpartei", ""),
        "Quelle": source,
        "Beleg": doc_ref,
        "Typ": doc_type,
        "Belegbetrag": doc_amount,
        "Beleg-Gegenpartei": counterparty,
        "Detail": detail,
        "Nachweiskette": logic,
        "Verwendungszweck": bank_row.get("Verwendungszweck", ""),
    }


def settlement_detail_for_source(doc: pd.Series, source: str) -> str:
    if source == "Plattform":
        label = platform_label(doc.get("platforms") or doc.get("platform")) or "Plattform"
        return label + ": " + join_nonempty([doc.get("platform_dates"), doc.get("platform_amounts"), doc.get("platform_order_ids"), doc.get("platform_counterparties"), doc.get("platform_payout_ids")])
    if source == "PayPal":
        return "PayPal: " + join_nonempty([doc.get("paypal_dates"), doc.get("paypal_amounts"), doc.get("paypal_counterparties")])
    return join_nonempty([doc.get("fyrst_dates"), doc.get("fyrst_amounts"), doc.get("fyrst_counterparties")])


def build_pdf(
    docs: pd.DataFrame,
    bank: pd.DataFrame,
    paypal: pd.DataFrame,
    evidence: pd.DataFrame,
    open_docs: pd.DataFrame,
    open_bank: pd.DataFrame,
    doc_report: pd.DataFrame,
    bank_report: pd.DataFrame,
    platform_transactions: pd.DataFrame | None = None,
    platform_package_report: pd.DataFrame | None = None,
    etsy_annual_report: pd.DataFrame | None = None,
    etsy_accountable_comparison: pd.DataFrame | None = None,
    leftover_candidate_report: pd.DataFrame | None = None,
    hypothesis_candidate_report: pd.DataFrame | None = None,
    ledger_experiment_report: pd.DataFrame | None = None,
    manual_edits: dict[str, dict[str, object]] | pd.DataFrame | None = None,
    manual_summary_note: str = "",
) -> None:
    manual_by_doc = normalize_manual_edits(manual_edits)
    if manual_by_doc:
        doc_report = build_doc_report(evidence, manual_by_doc)
    build_beleg_pdf(docs, bank, paypal, evidence, open_docs, open_bank, doc_report, platform_transactions, manual_summary_note)
    settlement_detail_report = build_settlement_detail_report(bank_report, evidence)
    build_control_pdf(
        bank_report,
        open_docs,
        open_bank,
        settlement_detail_report,
        platform_package_report,
        etsy_annual_report,
        etsy_accountable_comparison,
        leftover_candidate_report,
        hypothesis_candidate_report,
    )
    build_hypothesis_pdf(hypothesis_candidate_report, open_docs, open_bank, settlement_detail_report)
    build_ledger_experiment_pdf(ledger_experiment_report)


def build_beleg_pdf(
    docs: pd.DataFrame,
    bank: pd.DataFrame,
    paypal: pd.DataFrame,
    evidence: pd.DataFrame,
    open_docs: pd.DataFrame,
    open_bank: pd.DataFrame,
    doc_report: pd.DataFrame,
    platform_transactions: pd.DataFrame | None = None,
    manual_summary_note: str = "",
) -> None:
    pdf = SimpleDocTemplate(
        str(REPORT_PATH),
        pagesize=landscape(A4),
        rightMargin=0.8 * cm,
        leftMargin=0.8 * cm,
        topMargin=0.8 * cm,
        bottomMargin=0.8 * cm,
        title="Buchhaltungs Buddy Beleg-Report",
    )
    styles = get_styles()
    story: list = []

    complete = int((doc_report["Status"] == "vollständig belegt").sum())
    partial = int((doc_report["Status"] == "teilweise belegt").sum())
    open_count = int((doc_report["Status"] == "offen").sum())
    manual_count = int((doc_report.get("Manuell geaendert", pd.Series(dtype=str)) == "ja").sum())

    story.append(Paragraph("Buchhaltungs Buddy", styles["Title"]))
    story.append(Paragraph("Beleg-Report", styles["Subtitle"]))
    story.append(Paragraph(f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}", styles["Small"]))
    story.append(Spacer(1, 0.3 * cm))

    summary_rows = [
        ["Kennzahl", "Wert"],
        ["Accountable-Belege gesamt", len(docs)],
        ["FYRST-Umsätze gesamt", len(bank)],
        ["PayPal-Zeilen gesamt", len(paypal)],
        ["Plattformzeilen gesamt", len(platform_transactions) if platform_transactions is not None else 0],
        ["Vollständig belegte Belege", complete],
        ["Teilweise belegte Belege", partial],
        ["Offene Belege", open_count],
        ["Manuell geaenderte Belege", manual_count],
        ["Offene FYRST-Umsätze", len(open_bank)],
    ]
    story.append(Paragraph("1. Kurzfazit", styles["H1"]))
    story.append(make_table(summary_rows, [8 * cm, 3 * cm], header=True))
    if text(manual_summary_note):
        story.append(Spacer(1, 0.2 * cm))
        story.append(Paragraph("Manuelle Notiz", styles["H2"]))
        story.append(Paragraph(escape_pdf_text(manual_summary_note, 1200), styles["Body"]))

    story.append(PageBreak())
    story.append(Paragraph("2. Belegliste: Einnahmen und Ausgaben mit Umsatzmatch", styles["H1"]))
    add_doc_table(story, styles, "2.1 Einnahmen", doc_report[doc_report["Typ"] == "Einnahme"])
    add_doc_table(story, styles, "2.2 Ausgaben", doc_report[doc_report["Typ"] == "Ausgabe"])

    pdf.build(story, onFirstPage=footer, onLaterPages=footer)


def build_control_pdf(
    bank_report: pd.DataFrame,
    open_docs: pd.DataFrame,
    open_bank: pd.DataFrame,
    settlement_detail_report: pd.DataFrame | None = None,
    platform_package_report: pd.DataFrame | None = None,
    etsy_annual_report: pd.DataFrame | None = None,
    etsy_accountable_comparison: pd.DataFrame | None = None,
    leftover_candidate_report: pd.DataFrame | None = None,
    hypothesis_candidate_report: pd.DataFrame | None = None,
) -> None:
    pdf = SimpleDocTemplate(
        str(CONTROL_REPORT_PATH),
        pagesize=landscape(A4),
        rightMargin=0.8 * cm,
        leftMargin=0.8 * cm,
        topMargin=0.8 * cm,
        bottomMargin=0.8 * cm,
        title="Buchhaltungs Buddy Kontroll-Report",
    )
    styles = get_styles()
    story: list = []
    story.append(Paragraph("Buchhaltungs Buddy", styles["Title"]))
    story.append(Paragraph("Kontroll-Report", styles["Subtitle"]))
    story.append(Paragraph(f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}", styles["Small"]))
    story.append(Spacer(1, 0.3 * cm))

    story.append(Paragraph("3. Bankliste: FYRST-Umsätze mit Belegmatch", styles["H1"]))
    add_bank_table(story, styles, bank_report)

    story.append(PageBreak())
    add_bank_table(story, styles, bank_report[bank_report["Status"] == "offen"], title="4.2 Offene FYRST-Umsätze")

    if settlement_detail_report is not None and not settlement_detail_report.empty:
        story.append(PageBreak())
        story.append(Paragraph("5. Sammelumsätze und Einzüge im Detail", styles["H1"]))
        add_settlement_detail_table(story, styles, settlement_detail_report)

    if platform_package_report is not None and not platform_package_report.empty:
        story.append(PageBreak())
        story.append(Paragraph("6. Plattform-Abrechnungspakete", styles["H1"]))
        story.append(
            Paragraph(
                "Diese Kontrollliste zeigt, ob Plattform-Verkäufe, Accountable-Gebührenkandidaten und FYRST-Auszahlung rechnerisch zusammenpassen.",
                styles["Body"],
            )
        )
        add_package_table(story, styles, platform_package_report)

    if etsy_annual_report is not None and not etsy_annual_report.empty:
        story.append(PageBreak())
        story.append(Paragraph("7. Etsy-Jahresabgleich", styles["H1"]))
        annual = etsy_annual_report[etsy_annual_report["level"].isin(["year", "all_shops_year"])].copy()
        monthly = etsy_annual_report[etsy_annual_report["level"].eq("month")].copy()
        add_etsy_annual_table(story, styles, annual, "7.1 Jahressummen")
        add_etsy_annual_table(story, styles, monthly, "7.2 Monate")

    if etsy_accountable_comparison is not None and not etsy_accountable_comparison.empty:
        story.append(PageBreak())
        story.append(Paragraph("8. Etsy gegen Accountable", styles["H1"]))
        annual_comparison = etsy_accountable_comparison[etsy_accountable_comparison["level"].isin(["year", "all"])].copy()
        monthly_comparison = etsy_accountable_comparison[etsy_accountable_comparison["level"].eq("month")].copy()
        add_etsy_accountable_table(story, styles, annual_comparison, "8.1 Jahressummen")
        add_etsy_accountable_table(story, styles, monthly_comparison, "8.2 Monate")

    story.append(PageBreak())
    story.append(Paragraph("9. Restprüfung: offene Belege und offene Umsätze", styles["H1"]))
    add_open_docs_table(story, styles, open_docs, "9.1 Offene Belege")
    add_open_bank_raw_table(story, styles, open_bank, "9.2 Offene Bankumsätze")

    if platform_package_report is not None and not platform_package_report.empty:
        etsy_chain = platform_package_report[platform_package_report["platform"].astype(str).str.lower().eq("etsy")].copy()
        if not etsy_chain.empty:
            story.append(PageBreak())
            story.append(Paragraph("10. Etsy-Kettenprüfung: Belege, Orders, Gebühren, FYRST", styles["H1"]))
            story.append(
                Paragraph(
                    "Kontrolle je Etsy-Auszahlungsperiode: Plattform-Verkäufe und -Gebühren werden den Accountable-Belegen und der FYRST-Buchung gegenübergestellt.",
                    styles["Body"],
                )
            )
            add_etsy_chain_table(story, styles, etsy_chain)

    if hypothesis_candidate_report is not None and not hypothesis_candidate_report.empty:
        story.append(PageBreak())
        story.append(Paragraph("11. Hypothesen: offene Belege gegen Restbeträge / offene Umsätze", styles["H1"]))
        story.append(
            Paragraph(
                "Mögliche Kandidaten, nicht automatisch bestätigt. Diese Liste dient nur zur manuellen Prüfung.",
                styles["Body"],
            )
        )
        add_hypothesis_table(story, styles, hypothesis_candidate_report)

    pdf.build(story, onFirstPage=footer, onLaterPages=footer)


def build_hypothesis_pdf(
    hypothesis_candidate_report: pd.DataFrame | None,
    open_docs: pd.DataFrame,
    open_bank: pd.DataFrame,
    settlement_detail_report: pd.DataFrame | None = None,
) -> None:
    frame = hypothesis_candidate_report if hypothesis_candidate_report is not None else pd.DataFrame()
    pdf = SimpleDocTemplate(
        str(HYPOTHESIS_REPORT_PATH),
        pagesize=landscape(A4),
        rightMargin=0.8 * cm,
        leftMargin=0.8 * cm,
        topMargin=0.8 * cm,
        bottomMargin=0.8 * cm,
        title="Buchhaltungs Buddy Hypothesen-Report",
    )
    styles = get_styles()
    story: list = []
    story.append(Paragraph("Buchhaltungs Buddy", styles["Title"]))
    story.append(Paragraph("Hypothesen-Report", styles["Subtitle"]))
    story.append(Paragraph(f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}", styles["Small"]))
    story.append(Spacer(1, 0.3 * cm))

    residual_count = 0
    if settlement_detail_report is not None and not settlement_detail_report.empty and "Rest" in settlement_detail_report:
        residuals = settlement_detail_report.copy()
        residuals["_rest_abs"] = pd.to_numeric(residuals["Rest"], errors="coerce").abs()
        residual_count = int(residuals.loc[residuals["_rest_abs"] > 0.01, "tx_id"].astype(str).nunique()) if "tx_id" in residuals else int((residuals["_rest_abs"] > 0.01).sum())

    story.append(Paragraph("1. Kurzfazit", styles["H1"]))
    summary_rows = [
        ["Kennzahl", "Wert"],
        ["Offene Belege", len(open_docs)],
        ["Offene FYRST-Umsätze", len(open_bank)],
        ["Sammelumsatz-Restbeträge", residual_count],
        ["Prüfkandidaten", len(frame)],
    ]
    story.append(make_table(summary_rows, [8 * cm, 3 * cm], header=True))
    story.append(Spacer(1, 0.25 * cm))
    story.append(
        Paragraph(
            "Mögliche Kandidaten aus offenen Belegen gegen offene FYRST-Umsätze und Restbeträge aus Sammelumsätzen. Nichts davon wird automatisch als Match bestätigt.",
            styles["Body"],
        )
    )

    story.append(PageBreak())
    story.append(Paragraph("2. Offene Belege gegen Restbeträge / offene Umsätze", styles["H1"]))
    add_hypothesis_table(story, styles, frame)

    pdf.build(story, onFirstPage=footer, onLaterPages=footer)


def build_ledger_experiment_pdf(ledger_experiment_report: pd.DataFrame | None) -> None:
    frame = ledger_experiment_report if ledger_experiment_report is not None else pd.DataFrame()
    pdf = SimpleDocTemplate(
        str(LEDGER_EXPERIMENT_REPORT_PATH),
        pagesize=landscape(A4),
        rightMargin=0.8 * cm,
        leftMargin=0.8 * cm,
        topMargin=0.8 * cm,
        bottomMargin=0.8 * cm,
        title="Buchhaltungs Buddy Ledger-Experiment",
    )
    styles = get_styles()
    story: list = []
    story.append(Paragraph("Buchhaltungs Buddy", styles["Title"]))
    story.append(Paragraph("Ledger-Experimentreport", styles["Subtitle"]))
    story.append(Paragraph(f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}", styles["Small"]))
    story.append(Spacer(1, 0.3 * cm))

    story.append(Paragraph("1. Kurzfazit", styles["H1"]))
    if frame.empty:
        story.append(Paragraph("Keine nicht vollständig belegten Belege im Ledger-Experiment.", styles["Body"]))
        pdf.build(story, onFirstPage=footer, onLaterPages=footer)
        return

    summary = (
        frame.groupby("ledger_status", dropna=False)["doc_ref"]
        .count()
        .reset_index()
        .rename(columns={"ledger_status": "Ledger-Status", "doc_ref": "Belege"})
        .sort_values("Belege", ascending=False)
    )
    story.append(make_table([summary.columns.tolist()] + summary.astype(str).values.tolist(), [7.0 * cm, 2.0 * cm], header=True))
    story.append(Spacer(1, 0.25 * cm))
    story.append(
        Paragraph(
            "Dieser Bericht ist ein getrenntes Experiment: offene Belege gegen Ledger-Hinweise, Restbeträge und offene Umsätze. "
            "Die Einträge sind mögliche Kandidaten, nicht automatisch bestätigt. Er verändert keine Hauptmatches und ersetzt nicht die Belegliste im Beleg-Report.",
            styles["Body"],
        )
    )

    story.append(PageBreak())
    story.append(Paragraph("2. Ledger-Sicht auf nicht vollständig belegte Belege", styles["H1"]))
    add_ledger_experiment_table(story, styles, frame)
    pdf.build(story, onFirstPage=footer, onLaterPages=footer)


def add_doc_table(story: list, styles: dict, title: str, frame: pd.DataFrame) -> None:
    story.append(Paragraph(title, styles["H2"]))
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = ["Beleg", "Belegdatum", "Datum-Basis", "Betrag", "Gegenpartei", "Status", "FYRST-Umsatzmatch", "Detailspur", "Hinweis"]
    headers = ["Beleg", "Belegdatum", "Datum-Basis", "Betrag", "Gegenpartei", "Status", "FYRST-Umsatzmatch", "Detailspur", "Hinweis"]
    widths = [2.2 * cm, 1.6 * cm, 1.6 * cm, 1.5 * cm, 3.2 * cm, 2.1 * cm, 5.0 * cm, 4.8 * cm, 5.8 * cm]
    add_table(story, styles, frame, cols, headers, widths)


def normalize_manual_edits(manual_edits: dict[str, dict[str, object]] | pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if manual_edits is None:
        return {}
    if isinstance(manual_edits, pd.DataFrame):
        rows = manual_edits.to_dict("records")
        return {str(row.get("doc_id") or row.get("Beleg") or ""): row for row in rows if text(row.get("doc_id") or row.get("Beleg"))}
    normalized: dict[str, dict[str, object]] = {}
    for key, value in manual_edits.items():
        if isinstance(value, dict):
            normalized[str(key)] = value
    return normalized


def apply_manual_doc_edit(report_row: dict[str, object], evidence_row: pd.Series, manual_by_doc: dict[str, dict[str, object]]) -> None:
    edit = manual_by_doc.get(str(evidence_row.get("doc_id")))
    if not edit:
        return
    changed = False
    for field in ["Status", "FYRST-Umsatzmatch", "Detailspur", "Hinweis"]:
        value = text(edit.get(field))
        if value:
            report_row[field] = value
            changed = True
    if changed:
        report_row["Manuell geaendert"] = "ja"
        if not text(edit.get("Hinweis")) and "Manuell" not in text(report_row.get("Hinweis")):
            report_row["Hinweis"] = join_nonempty([report_row.get("Hinweis"), "Manuell im Belegreport angepasst."])


def is_marketplace_settlement_doc(doc: pd.Series) -> bool:
    return is_marketplace_settlement_text(
        doc.get("platforms"),
        doc.get("platform"),
        doc.get("platform_counterparties"),
        doc.get("platform_order_ids"),
        doc.get("platform_payout_ids"),
        doc.get("evidence_path"),
    )


def is_marketplace_settlement_text(*values: object) -> bool:
    combined = " ".join(text(value).lower() for value in values if text(value))
    return "etsy" in combined or "ebay" in combined


def add_bank_table(story: list, styles: dict, frame: pd.DataFrame, title: str | None = None) -> None:
    if title:
        story.append(Paragraph(title, styles["H2"]))
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = ["FYRST-Datum", "FYRST-Betrag", "Belegsumme", "Differenz", "Gegenpartei", "Status", "Belegmatches", "Matchlogik", "Verwendungszweck"]
    headers = ["FYRST-Datum", "Umsatz", "Belegsumme", "Rest", "Gegenpartei", "Status", "Belege", "Matchlogik", "Verwendungszweck"]
    widths = [1.8 * cm, 1.6 * cm, 1.7 * cm, 1.4 * cm, 3.8 * cm, 1.6 * cm, 3.7 * cm, 4.6 * cm, 6.8 * cm]
    add_table(story, styles, frame, cols, headers, widths)


def add_package_table(story: list, styles: dict, frame: pd.DataFrame) -> None:
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = [
        "platform",
        "payout_date",
        "bank_amount",
        "platform_detail_net",
        "linked_doc_sum",
        "candidate_fee_doc_sum",
        "package_doc_sum",
        "gap_to_bank",
        "package_status",
        "note",
    ]
    headers = ["Plattform", "Auszahlung", "FYRST", "Plattform-Netto", "Belege", "Gebührenkand.", "Paketsumme", "Diff.", "Status", "Hinweis"]
    widths = [1.6 * cm, 1.8 * cm, 1.6 * cm, 2.0 * cm, 1.8 * cm, 2.2 * cm, 1.8 * cm, 1.4 * cm, 3.2 * cm, 10.0 * cm]
    add_table(story, styles, frame, cols, headers, widths)


def add_etsy_chain_table(story: list, styles: dict, frame: pd.DataFrame) -> None:
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    work = frame.copy()
    work["Hinweis kurz"] = work.apply(etsy_chain_note, axis=1)
    cols = [
        "payout_date",
        "payout_id",
        "bank_date",
        "bank_amount",
        "platform_order_amount",
        "platform_fee_amount",
        "platform_detail_net",
        "linked_doc_sum",
        "candidate_fee_doc_sum",
        "gap_to_bank",
        "package_status",
        "linked_doc_refs",
        "candidate_fee_doc_refs",
        "Hinweis kurz",
    ]
    headers = [
        "Periode",
        "Payout",
        "FYRST",
        "FYRST-Betrag",
        "Etsy-Verkäufe",
        "Etsy-Gebühren",
        "Etsy-Netto",
        "Accountable-Belege",
        "Gebührenkandidaten",
        "Diff.",
        "Status",
        "Ausgangsbelege",
        "Gebührenbelege",
        "Hinweis",
    ]
    widths = [
        1.35 * cm,
        2.05 * cm,
        1.35 * cm,
        1.25 * cm,
        1.35 * cm,
        1.35 * cm,
        1.25 * cm,
        1.45 * cm,
        1.45 * cm,
        1.05 * cm,
        2.05 * cm,
        2.95 * cm,
        3.25 * cm,
        3.45 * cm,
    ]
    add_table(story, styles, work, cols, headers, widths)


def etsy_chain_note(row: pd.Series) -> str:
    status = text(row.get("package_status"))
    if status == "candidate_fees_do_not_close":
        return "Gebührenkandidaten vorhanden; Summe passt noch nicht zur Auszahlung."
    if status == "no_bank_bridge":
        return "Etsy-Auszahlung ohne passende FYRST-Buchung im Upload."
    if status == "no_accountable_sales_link":
        return "Etsy-Verkäufe vorhanden; passende Accountable-Ausgangsbelege fehlen."
    return text(row.get("note"))


def add_settlement_detail_table(story: list, styles: dict, frame: pd.DataFrame) -> None:
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    work = frame.copy()
    work["_group_key"] = work.apply(
        lambda row: text(row.get("tx_id"))
        or "|".join(
            [
                text(row.get("FYRST-Datum")),
                text(row.get("FYRST-Betrag")),
                text(row.get("FYRST-Gegenpartei")),
                text(row.get("Verwendungszweck")),
            ]
        ),
        axis=1,
    )
    work["_sort_date"] = pd.to_datetime(work["FYRST-Datum"], errors="coerce")
    work["_sort_amount"] = pd.to_numeric(work["FYRST-Betrag"], errors="coerce")
    work = work.sort_values(["_sort_date", "_sort_amount", "FYRST-Gegenpartei", "Beleg"], na_position="last")

    cols = ["Quelle", "Beleg", "Typ", "Belegbetrag", "Beleg-Gegenpartei", "Detail", "Nachweiskette"]
    headers = ["Quelle", "Beleg", "Typ", "Betrag", "Gegenpartei", "Detail", "Nachweiskette"]
    widths = [2.0 * cm, 2.1 * cm, 1.4 * cm, 1.5 * cm, 3.4 * cm, 10.0 * cm, 5.8 * cm]

    for _, group in work.groupby("_group_key", sort=False):
        first = group.iloc[0]
        heading = (
            f"{display_date(first.get('FYRST-Datum'))} | "
            f"FYRST {format_amount(first.get('FYRST-Betrag'))} EUR | "
            f"{text(first.get('Plattform')) or 'ohne Plattform'} | "
            f"{text(first.get('FYRST-Gegenpartei'))}"
        )
        story.append(Paragraph(escape_pdf_text(heading, 320), styles["H2"]))
        summary = (
            f"Belegsumme: {format_amount(first.get('Belegsumme'))} EUR | "
            f"Restbetrag: {format_amount(first.get('Rest'))} EUR"
        )
        story.append(Paragraph(escape_pdf_text(summary, 260), styles["Small"]))
        purpose = text(first.get("Verwendungszweck"))
        if purpose:
            story.append(Paragraph(escape_pdf_text(f"Verwendungszweck: {purpose}", 420), styles["Small"]))

        rows = [headers]
        details = group.sort_values(["Quelle", "Beleg"], na_position="last")
        for _, row in details.iterrows():
            rows.append([format_cell(row.get(col, ""), styles["Tiny"]) for col in cols])
        story.append(make_table(rows, widths, header=True))
        story.append(Spacer(1, 0.35 * cm))


def add_etsy_annual_table(story: list, styles: dict, frame: pd.DataFrame, title: str) -> None:
    story.append(Paragraph(title, styles["H2"]))
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = [
        "level",
        "shop",
        "period",
        "sales",
        "fees",
        "taxes",
        "adjustments",
        "payouts",
        "calculated_balance_change",
        "status",
    ]
    headers = ["Ebene", "Shop", "Zeitraum", "Verkäufe", "Gebühren", "Steuern", "Korrekturen", "Auszahlungen", "Saldo", "Status"]
    widths = [2.0 * cm, 1.8 * cm, 1.7 * cm, 2.0 * cm, 2.0 * cm, 2.0 * cm, 2.1 * cm, 2.2 * cm, 1.7 * cm, 2.3 * cm]
    add_table(story, styles, frame, cols, headers, widths)


def add_etsy_accountable_table(story: list, styles: dict, frame: pd.DataFrame, title: str) -> None:
    story.append(Paragraph(title, styles["H2"]))
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = [
        "level",
        "period",
        "statement_sales",
        "accountable_income",
        "sales_diff",
        "statement_costs",
        "accountable_expenses",
        "costs_diff",
        "statement_net_before_payout",
        "accountable_net",
        "net_diff",
        "status",
    ]
    headers = ["Ebene", "Zeitraum", "Etsy Verkäufe", "Accountable Einnahmen", "Diff.", "Etsy Kosten", "Accountable Ausgaben", "Diff.", "Etsy Netto", "Accountable Netto", "Diff.", "Status"]
    widths = [1.4 * cm, 1.6 * cm, 2.0 * cm, 2.3 * cm, 1.4 * cm, 1.9 * cm, 2.3 * cm, 1.4 * cm, 1.9 * cm, 2.2 * cm, 1.4 * cm, 1.5 * cm]
    add_table(story, styles, frame, cols, headers, widths)


def add_leftover_table(story: list, styles: dict, frame: pd.DataFrame) -> None:
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = ["doc_ref", "doc_date", "doc_amount", "doc_counterparty", "bank_date", "bank_amount", "bank_counterparty", "amount_diff", "day_diff", "score", "recommendation"]
    headers = ["Beleg", "Belegdatum", "Beleg", "Beleg-Gegenpartei", "Bankdatum", "Umsatz", "Bank-Gegenpartei", "Diff.", "Tage", "Score", "Empfehlung"]
    widths = [2.0 * cm, 1.7 * cm, 1.4 * cm, 3.2 * cm, 1.7 * cm, 1.4 * cm, 3.5 * cm, 1.2 * cm, 1.0 * cm, 1.2 * cm, 4.3 * cm]
    add_table(story, styles, frame.head(80), cols, headers, widths)


def add_hypothesis_table(story: list, styles: dict, frame: pd.DataFrame) -> None:
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    work = frame.copy()
    work["doc_type"] = work["doc_type"].map({"income": "Einnahme", "expense": "Ausgabe"}).fillna(work["doc_type"]) if "doc_type" in work else ""
    cols = [
        "hypothesis_source",
        "doc_ref",
        "doc_type",
        "doc_date",
        "doc_amount",
        "doc_counterparty",
        "source_date",
        "source_rest_amount",
        "source_counterparty",
        "amount_diff",
        "day_diff",
        "score",
        "recommendation",
    ]
    headers = ["Quelle", "Beleg", "Typ", "Belegdatum", "Beleg", "Beleg-Gegenpartei", "Umsatz/Rest", "Rest/Umsatz", "Umsatz-Gegenpartei", "Diff.", "Tage", "Score", "Empfehlung"]
    widths = [2.7 * cm, 2.0 * cm, 1.3 * cm, 1.6 * cm, 1.4 * cm, 3.2 * cm, 1.7 * cm, 1.5 * cm, 3.4 * cm, 1.1 * cm, 0.9 * cm, 1.0 * cm, 4.5 * cm]
    add_table(story, styles, work.head(180), cols, headers, widths)


def add_ledger_experiment_table(story: list, styles: dict, frame: pd.DataFrame) -> None:
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    work = frame.copy()
    work["doc_type"] = work["doc_type"].map({"income": "Einnahme", "expense": "Ausgabe"}).fillna(work["doc_type"]) if "doc_type" in work else ""
    cols = [
        "ledger_status",
        "doc_ref",
        "doc_type",
        "date",
        "amount",
        "platform",
        "counterparty",
        "main_status",
        "ledger_source",
        "ledger_date",
        "ledger_amount",
        "ledger_category",
        "bank_bridge",
        "recommendation",
    ]
    headers = ["Ledger-Status", "Beleg", "Typ", "Datum", "Betrag", "Plattform", "Gegenpartei", "Hauptstatus", "Ledger", "Ledger-Datum", "Ledger-Betrag", "Kategorie", "Bank", "Empfehlung"]
    widths = [3.0 * cm, 1.9 * cm, 1.2 * cm, 1.5 * cm, 1.2 * cm, 1.3 * cm, 2.7 * cm, 2.0 * cm, 1.8 * cm, 1.5 * cm, 1.5 * cm, 1.8 * cm, 1.0 * cm, 4.2 * cm]
    add_table(story, styles, work, cols, headers, widths)


def add_open_docs_table(story: list, styles: dict, frame: pd.DataFrame, title: str) -> None:
    story.append(Paragraph(title, styles["H2"]))
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    work = frame.copy()
    if "signed_amount" not in work and "amount" in work:
        work["signed_amount"] = work["amount"]
    cols = ["doc_ref", "doc_type", "date", "date_source", "signed_amount", "platform", "counterparty", "description"]
    headers = ["Beleg", "Typ", "Belegdatum", "Datum-Basis", "Betrag", "Plattform", "Gegenpartei", "Beschreibung"]
    widths = [2.0 * cm, 1.5 * cm, 1.6 * cm, 1.7 * cm, 1.5 * cm, 1.5 * cm, 3.8 * cm, 10.0 * cm]
    add_table(story, styles, work, cols, headers, widths)


def add_open_bank_raw_table(story: list, styles: dict, frame: pd.DataFrame, title: str) -> None:
    story.append(Paragraph(title, styles["H2"]))
    if frame.empty:
        story.append(Paragraph("Keine Zeilen.", styles["Body"]))
        return
    cols = ["source", "date", "amount", "counterparty", "description", "currency"]
    headers = ["Quelle", "Datum", "Umsatz", "Gegenpartei", "Verwendungszweck", "Währung"]
    widths = [1.6 * cm, 1.7 * cm, 1.5 * cm, 4.2 * cm, 14.0 * cm, 1.2 * cm]
    add_table(story, styles, frame, cols, headers, widths)


def add_table(story: list, styles: dict, frame: pd.DataFrame, cols: list[str], headers: list[str], widths: list[float]) -> None:
    subset = frame.sort_values(cols[1] if len(cols) > 1 else cols[0]).copy()
    rows = [headers]
    for _, row in subset.iterrows():
        rows.append([format_cell(row.get(col, ""), styles["Tiny"]) for col in cols])
    story.append(make_table(rows, widths, header=True))
    story.append(Spacer(1, 0.25 * cm))


def make_table(rows: list[list[object]], widths: list[float], header: bool = False) -> Table:
    table = Table(rows, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
    style = [
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 5.7),
        ("LEADING", (0, 0), (-1, -1), 6.7),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D1D5DB")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]
    if header:
        style.extend(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E5E7EB")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
    table.setStyle(TableStyle(style))
    return table


def format_cell(value: object, style: ParagraphStyle) -> Paragraph:
    if value is None or pd.isna(value):
        rendered = ""
    elif isinstance(value, pd.Timestamp):
        rendered = value.strftime("%Y-%m-%d")
    elif isinstance(value, float):
        rendered = f"{value:.2f}" if math.isfinite(value) else ""
    else:
        rendered = str(value)
    return Paragraph(escape_pdf_text(rendered), style)


def escape_pdf_text(value: str, limit: int = 260) -> str:
    text_value = " ".join(str(value).replace("\n", " ").replace("\r", " ").split())
    if len(text_value) > limit:
        text_value = text_value[: limit - 1] + "…"
    return text_value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def split_ids(value: object) -> list[str]:
    return [item.strip() for item in str(value or "").split("|") if item.strip() and item.strip() != "nan"]


def text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def join_nonempty(values) -> str:
    return " / ".join(text(value) for value in values if text(value))


def join_unique(values) -> str:
    seen: list[str] = []
    for value in values:
        value_text = text(value)
        if not value_text or value_text in seen:
            continue
        seen.append(value_text)
    return " | ".join(seen)


def format_amount(value: object) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return ""


def display_date(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    try:
        return pd.Timestamp(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def platform_label(value: object) -> str:
    labels = []
    mapping = {
        "etsy": "Etsy",
        "ebay": "eBay",
        "paypal": "PayPal",
        "shopify": "Shopify",
        "printful": "Printful",
        "gelato": "Gelato",
    }
    for item in split_ids(value):
        label = mapping.get(item.lower(), item)
        if label and label not in labels:
            labels.append(label)
    return " + ".join(labels)


def get_styles() -> dict:
    base = getSampleStyleSheet()
    return {
        "Title": ParagraphStyle("Title", parent=base["Title"], fontName="Helvetica-Bold", fontSize=22, leading=26, textColor=colors.HexColor("#111827")),
        "Subtitle": ParagraphStyle("Subtitle", parent=base["Normal"], fontName="Helvetica", fontSize=11, leading=14, textColor=colors.HexColor("#4B5563")),
        "H1": ParagraphStyle("H1", parent=base["Heading1"], fontName="Helvetica-Bold", fontSize=14, leading=18, spaceBefore=8, spaceAfter=6, textColor=colors.HexColor("#111827")),
        "H2": ParagraphStyle("H2", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=10, leading=13, spaceBefore=5, spaceAfter=4, textColor=colors.HexColor("#1F2937")),
        "Body": ParagraphStyle("Body", parent=base["BodyText"], fontName="Helvetica", fontSize=9, leading=12, textColor=colors.HexColor("#1F2937"), alignment=TA_LEFT),
        "Small": ParagraphStyle("Small", parent=base["BodyText"], fontName="Helvetica", fontSize=8, leading=10, textColor=colors.HexColor("#6B7280")),
        "Tiny": ParagraphStyle("Tiny", parent=base["BodyText"], fontName="Helvetica", fontSize=5.3, leading=6.3, textColor=colors.HexColor("#111827")),
    }


def footer(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.HexColor("#6B7280"))
    canvas.drawString(0.8 * cm, 0.4 * cm, "Buchhaltungs Buddy")
    canvas.drawRightString(landscape(A4)[0] - 0.8 * cm, 0.4 * cm, f"Seite {doc.page}")
    canvas.restoreState()


if __name__ == "__main__":
    raise SystemExit(main())
