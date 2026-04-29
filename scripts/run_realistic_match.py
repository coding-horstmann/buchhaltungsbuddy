from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import build_explicit_match_report as report_pdf  # noqa: E402
from reconcile.annual_reports import build_etsy_accountable_comparison, build_etsy_annual_pdf, build_etsy_annual_reconciliation  # noqa: E402
from reconcile.evidence import build_document_evidence, build_open_bank_after_evidence  # noqa: E402
from reconcile.leftovers import build_hypothesis_candidate_report, build_leftover_candidate_report  # noqa: E402
from reconcile.ledger_experiment import build_ledger_experiment_report  # noqa: E402
from reconcile.matching import MatchSettings, reconcile, text_similarity  # noqa: E402
from reconcile.parsers import parse_accountable_file, parse_bank_file, parse_paypal_file  # noqa: E402
from reconcile.paypal import match_docs_to_paypal, match_paypal_transfers_to_bank  # noqa: E402
from reconcile.platform_packages import build_platform_package_matches, canonical_platform  # noqa: E402
from reconcile.platforms import (  # noqa: E402
    assign_platform_payout_ids,
    deduplicate_platform_transactions,
    match_docs_to_platform,
    match_platform_payouts_to_bank,
    parse_platform_file,
)
from reconcile.plausibility import build_overall_plausibility_pdf, build_overall_plausibility_report  # noqa: E402
from reconcile.usage import build_bank_claim_usage  # noqa: E402


SOURCE_DIR = Path(r"C:\Users\PC\Downloads\csv match")
EXTRA_SOURCE_DIR = Path(r"C:\Users\PC\Downloads\weitere")
ADDITIONAL_PLATFORM_FILES = [
    Path(r"C:\Users\PC\Downloads\ebay transaktionsbericht.csv"),
]
OUTPUT_DIR = ROOT / "outputs" / "realistic_run"


def first_existing_path(names: list[str], folders: list[Path], required: bool = True) -> Path | None:
    for folder in folders:
        for name in names:
            candidate = folder / name
            if candidate.exists():
                return candidate
    if required:
        searched = ", ".join(str(folder / name) for folder in folders for name in names)
        raise FileNotFoundError(f"Keine Eingabedatei gefunden. Gesucht: {searched}")
    return None


def collect_platform_paths(folders: list[Path]) -> list[Path]:
    excluded = {"fyrst.csv", "paypal.csv"}
    paths: list[Path] = []
    seen: set[str] = set()
    for folder in folders:
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.csv")):
            if path.name.lower() in excluded:
                continue
            key = str(path.resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            paths.append(path)
    for path in ADDITIONAL_PLATFORM_FILES:
        if not path.exists():
            continue
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        paths.append(path)
    return paths


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    settings = MatchSettings()

    accountable_path = first_existing_path(["Daten.xlsx", "accountable.xlsx"], [SOURCE_DIR, ROOT])
    bank_paths = [first_existing_path(["fyrst.csv"], [SOURCE_DIR, ROOT])]
    paypal_path = first_existing_path(["paypal.CSV", "paypal.csv"], [SOURCE_DIR, ROOT], required=False)
    platform_paths = collect_platform_paths([SOURCE_DIR, EXTRA_SOURCE_DIR])

    issues: list[str] = []
    accountable = parse_accountable_file(accountable_path.name, accountable_path.read_bytes())
    issues.extend(accountable.issues)
    docs = accountable.frame

    bank_frames = []
    for path in bank_paths:
        parsed = parse_bank_file(path.name, path.read_bytes())
        issues.extend(parsed.issues)
        if not parsed.frame.empty:
            bank_frames.append(parsed.frame)
    bank = pd.concat(bank_frames, ignore_index=True) if bank_frames else pd.DataFrame()

    paypal = pd.DataFrame()
    if paypal_path is not None and paypal_path.exists():
        parsed_paypal = parse_paypal_file(paypal_path.name, paypal_path.read_bytes())
        issues.extend(parsed_paypal.issues)
        paypal = parsed_paypal.frame

    platform_frames = []
    for path in platform_paths:
        parsed_platform = parse_platform_file(path.name, path.read_bytes())
        issues.extend(parsed_platform.issues)
        if not parsed_platform.frame.empty:
            platform_frames.append(parsed_platform.frame)
    platform_transactions = pd.concat(platform_frames, ignore_index=True) if platform_frames else pd.DataFrame()
    platform_transactions = assign_platform_payout_ids(platform_transactions)
    platform_transactions = deduplicate_platform_transactions(platform_transactions)

    matches, links = reconcile(docs, bank, settings)
    paypal_doc_matches, paypal_doc_links = match_docs_to_paypal(docs, paypal, settings)
    paypal_bank_matches = match_paypal_transfers_to_bank(paypal, bank, settings)
    platform_doc_matches, platform_doc_links = match_docs_to_platform(docs, platform_transactions, settings)
    platform_bank_matches = match_platform_payouts_to_bank(platform_transactions, bank, settings)
    platform_package_matches, platform_package_links, platform_package_report = build_platform_package_matches(
        docs,
        bank,
        matches,
        links,
        platform_transactions,
        platform_doc_matches,
        platform_doc_links,
        platform_bank_matches,
        settings,
    )
    if not platform_package_matches.empty:
        matches = pd.concat([matches, platform_package_matches], ignore_index=True)
        links = pd.concat([links, platform_package_links], ignore_index=True)

    evidence = build_document_evidence(
        docs,
        bank,
        paypal,
        matches,
        links,
        paypal_doc_matches,
        paypal_doc_links,
        paypal_bank_matches,
        platform_transactions,
        platform_doc_matches,
        platform_doc_links,
        platform_bank_matches,
    )
    open_doc_ids = set(evidence.loc[evidence["evidence_level"] == "offen", "doc_id"])
    open_docs = docs[docs["doc_id"].isin(open_doc_ids)].copy()
    open_bank = build_open_bank_after_evidence(bank, matches, paypal_bank_matches, platform_bank_matches)

    doc_report = report_pdf.build_doc_report(evidence)
    bank_report = report_pdf.build_bank_report(bank, evidence, matches, paypal_bank_matches, platform_bank_matches)
    settlement_detail_report = report_pdf.build_settlement_detail_report(bank_report, evidence)
    bank_claim_usage = build_bank_claim_usage(evidence, bank)
    overall_plausibility_report = build_overall_plausibility_report(docs, bank, paypal, evidence, bank_claim_usage)
    payout_report = build_platform_payout_reconciliation(platform_transactions, platform_bank_matches)
    etsy_annual_report = build_etsy_annual_reconciliation(platform_transactions)
    etsy_accountable_comparison = build_etsy_accountable_comparison(evidence, platform_transactions)
    leftover_candidate_report = build_leftover_candidate_report(open_docs, open_bank, settings)
    hypothesis_candidate_report = build_hypothesis_candidate_report(open_docs, open_bank, settlement_detail_report, settings)
    ledger_experiment_report = build_ledger_experiment_report(evidence)
    alias_report = build_alias_control(docs)
    ai_doc_candidates = build_ai_doc_candidates(open_docs, open_bank)
    ai_bank_candidates = build_ai_bank_candidates(open_bank, open_docs, settings)
    ai_summary = build_ai_summary(evidence, bank_report, payout_report, ai_doc_candidates, ai_bank_candidates)

    write_csv(doc_report, "belege_mit_umsatzmatch.csv")
    write_csv(bank_report, "umsaetze_mit_belegmatch.csv")
    write_csv(settlement_detail_report, "sammelumsaetze_detail_report.csv")
    write_csv(bank_claim_usage, "bank_claim_usage.csv")
    write_csv(overall_plausibility_report, "overall_plausibility_report.csv")
    write_csv(evidence, "document_evidence_chains.csv")
    write_csv(bank, "bank_transactions_normalized.csv")
    write_csv(matches, "fyrst_matches.csv")
    write_csv(links, "fyrst_match_links.csv")
    write_csv(paypal, "paypal_transactions_normalized.csv")
    write_csv(paypal_doc_matches, "paypal_doc_matches.csv")
    write_csv(paypal_doc_links, "paypal_doc_links.csv")
    write_csv(paypal_bank_matches, "paypal_bank_bridge.csv")
    write_csv(platform_transactions, "platform_transactions_normalized.csv")
    write_csv(platform_doc_matches, "platform_doc_matches.csv")
    write_csv(platform_doc_links, "platform_doc_links.csv")
    write_csv(platform_bank_matches, "platform_bank_bridge.csv")
    write_csv(platform_package_matches, "platform_package_matches.csv")
    write_csv(platform_package_links, "platform_package_links.csv")
    write_csv(platform_package_report, "platform_package_report.csv")
    write_csv(open_docs, "open_docs.csv")
    write_csv(open_bank, "open_bank.csv")
    write_csv(payout_report, "platform_payout_reconciliation.csv")
    write_csv(etsy_annual_report, "etsy_annual_reconciliation.csv")
    write_csv(etsy_accountable_comparison, "etsy_accountable_comparison.csv")
    write_csv(leftover_candidate_report, "leftover_candidate_report.csv")
    write_csv(hypothesis_candidate_report, "hypothesis_candidate_report.csv")
    write_csv(ledger_experiment_report, "ledger_experiment_report.csv")
    write_csv(alias_report, "alias_control.csv")
    write_csv(ai_doc_candidates, "ai_review_doc_candidates.csv")
    write_csv(ai_bank_candidates, "ai_review_bank_candidates.csv")
    write_csv(ai_summary, "ai_review_summary.csv")
    (OUTPUT_DIR / "import_issues.txt").write_text("\n".join(issues), encoding="utf-8")

    report_pdf.REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_beleg_report.pdf"
    report_pdf.CONTROL_REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_kontroll_report.pdf"
    report_pdf.HYPOTHESIS_REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_hypothesen_report.pdf"
    report_pdf.LEDGER_EXPERIMENT_REPORT_PATH = OUTPUT_DIR / "buchhaltungs_buddy_ledger_experiment_report.pdf"
    report_pdf.build_pdf(
        docs,
        bank,
        paypal,
        evidence,
        open_docs,
        open_bank,
        doc_report,
        bank_report,
        platform_transactions=platform_transactions,
        platform_package_report=platform_package_report,
        etsy_annual_report=etsy_annual_report,
        etsy_accountable_comparison=etsy_accountable_comparison,
        leftover_candidate_report=leftover_candidate_report,
        hypothesis_candidate_report=hypothesis_candidate_report,
        ledger_experiment_report=ledger_experiment_report,
    )
    etsy_pdf_path = OUTPUT_DIR / "etsy_jahresabgleich.pdf"
    build_etsy_annual_pdf(etsy_annual_report, etsy_pdf_path)
    plausibility_pdf_path = OUTPUT_DIR / "gesamt_plausibilitaetsbericht.pdf"
    build_overall_plausibility_pdf(overall_plausibility_report, plausibility_pdf_path)

    summary = {
        "source_dir": str(SOURCE_DIR),
        "extra_source_dir": str(EXTRA_SOURCE_DIR),
        "platform_files": len(platform_paths),
        "accountable_docs": len(docs),
        "bank_transactions": len(bank),
        "paypal_rows": len(paypal),
        "platform_rows": len(platform_transactions),
        "fyrst_matches": len(matches),
        "fyrst_linked_docs": int(links["doc_id"].nunique()) if not links.empty else 0,
        "paypal_doc_matches": len(paypal_doc_matches),
        "paypal_doc_linked_docs": int(paypal_doc_links["doc_id"].nunique()) if not paypal_doc_links.empty else 0,
        "paypal_bank_bridge": len(paypal_bank_matches),
        "platform_doc_matches": len(platform_doc_matches),
        "platform_doc_linked_docs": int(platform_doc_links["doc_id"].nunique()) if not platform_doc_links.empty else 0,
        "platform_bank_bridge": len(platform_bank_matches),
        "platform_package_matches": len(platform_package_matches),
        "platform_package_auto": int((platform_package_report["package_status"] == "auto_matched").sum()) if not platform_package_report.empty else 0,
        "complete_docs": int((evidence["evidence_level"] == "vollständig belegt").sum()),
        "partial_docs": int((evidence["evidence_level"] == "teilweise belegt").sum()),
        "open_docs": len(open_docs),
        "open_bank": len(open_bank),
        "bank_gross_platform_over_bank": int((bank_claim_usage["usage_status"] == "gross_platform_over_bank").sum()) if not bank_claim_usage.empty else 0,
        "bank_hard_over_claim": int((bank_claim_usage["usage_status"] == "over_claim").sum()) if not bank_claim_usage.empty else 0,
        "leftover_candidates": len(leftover_candidate_report),
        "hypothesis_candidates": len(hypothesis_candidate_report),
        "ledger_experiment_rows": len(ledger_experiment_report),
        "beleg_report_pdf": str(report_pdf.REPORT_PATH),
        "kontroll_report_pdf": str(report_pdf.CONTROL_REPORT_PATH),
        "hypothesen_report_pdf": str(report_pdf.HYPOTHESIS_REPORT_PATH),
        "ledger_experiment_pdf": str(report_pdf.LEDGER_EXPERIMENT_REPORT_PATH),
        "etsy_annual_pdf": str(etsy_pdf_path),
        "overall_plausibility_pdf": str(plausibility_pdf_path),
    }
    write_csv(pd.DataFrame([summary]), "summary.csv")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(f"Beleg-PDF: {report_pdf.REPORT_PATH}")
    print(f"Kontroll-PDF: {report_pdf.CONTROL_REPORT_PATH}")
    return 0


def write_csv(frame: pd.DataFrame, name: str) -> None:
    frame.to_csv(OUTPUT_DIR / name, sep=";", index=False, encoding="utf-8-sig")


def build_alias_control(docs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if docs.empty:
        return pd.DataFrame(columns=["canonical_platform", "counterparty", "doc_type", "count", "sum"])
    work = docs.copy()
    work["canonical_platform"] = work.apply(
        lambda row: canonical_platform(row.get("platform"), row.get("counterparty"), row.get("description")),
        axis=1,
    )
    grouped = (
        work.groupby(["canonical_platform", "counterparty", "doc_type"], dropna=False)["signed_amount"]
        .agg(["count", "sum"])
        .reset_index()
        .sort_values(["canonical_platform", "counterparty", "doc_type"])
    )
    for _, row in grouped.iterrows():
        rows.append(
            {
                "canonical_platform": row.get("canonical_platform", ""),
                "counterparty": row.get("counterparty", ""),
                "doc_type": row.get("doc_type", ""),
                "count": int(row.get("count") or 0),
                "sum": round(float(row.get("sum") or 0), 2),
            }
        )
    return pd.DataFrame(rows)


def build_platform_payout_reconciliation(platform_transactions: pd.DataFrame, platform_bank_matches: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "payout_id",
        "platform",
        "payout_date",
        "platform_payout_amount",
        "expected_bank_amount",
        "detail_net_amount",
        "detail_rows",
        "bank_amount",
        "bank_date",
        "bank_tx_id",
        "detail_vs_bank_delta",
        "platform_vs_bank_delta",
        "status",
        "note",
    ]
    if platform_transactions.empty:
        return pd.DataFrame(columns=columns)

    details = platform_transactions[
        platform_transactions["category"].isin(["order", "fee", "charge", "refund", "adjustment", "ledger_order", "ledger_fee", "ledger_tax", "ledger_adjustment"])
    ].copy()
    payouts = platform_transactions[platform_transactions["category"].isin(["payout", "bank_transfer"])].copy()
    if payouts.empty:
        return pd.DataFrame(columns=columns)
    if not payouts.empty:
        payouts["_source_rank"] = payouts["source"].astype(str).eq("Etsy Statement").astype(int)
        payouts = (
            payouts.sort_values(["platform", "payout_id", "_source_rank"], ascending=[True, True, False])
            .drop_duplicates(["platform", "payout_id"], keep="first")
            .drop(columns=["_source_rank"], errors="ignore")
        )

    bridges_by_platform_tx = {
        key: group.copy()
        for key, group in platform_bank_matches.groupby("platform_tx_id", dropna=False)
    } if not platform_bank_matches.empty else {}

    rows = []
    for _, payout in payouts.sort_values(["platform", "date", "amount"]).iterrows():
        payout_id = str(payout.get("payout_id") or "")
        detail_rows = details[details["payout_id"].astype(str).eq(payout_id)] if payout_id else details.iloc[0:0]
        if str(payout.get("platform") or "").lower() == "etsy" and not detail_rows.empty:
            statement_rows = detail_rows[detail_rows["source"].astype(str).eq("Etsy Statement")].copy()
            if not statement_rows.empty:
                detail_rows = statement_rows
        bridge_rows = bridges_by_platform_tx.get(payout.get("tx_id"), pd.DataFrame())
        expected_bank = round(-float(payout.get("amount", 0)), 2)
        detail_net = round(float(detail_rows["amount"].sum()), 2) if not detail_rows.empty else 0.0
        bank_amount = round(float(bridge_rows["bank_amount"].sum()), 2) if not bridge_rows.empty else 0.0
        detail_delta = round(detail_net - bank_amount, 2) if bank_amount else None
        platform_delta = round(expected_bank - bank_amount, 2) if bank_amount else None
        if bridge_rows.empty:
            status = "no_bank_bridge"
            note = "Payout exists in platform export, but no FYRST transfer was found."
        elif detail_rows.empty:
            status = "bank_bridge_only"
            note = "Payout matches FYRST, but no individual order/fee details are linked to this payout."
        elif abs(detail_delta or 0) <= 0.05 and abs(platform_delta or 0) <= 0.05:
            status = "balanced"
            note = "Order/fee net, platform payout and FYRST transfer balance within tolerance."
        elif abs(platform_delta or 0) <= 0.05:
            status = "bank_balanced_details_gap"
            note = "Platform payout matches FYRST, but linked order/fee rows do not fully explain the payout."
        else:
            status = "needs_review"
            note = "Payout needs manual review."
        rows.append(
            {
                "payout_id": payout_id,
                "platform": payout.get("platform", ""),
                "payout_date": payout.get("date"),
                "platform_payout_amount": round(float(payout.get("amount", 0)), 2),
                "expected_bank_amount": expected_bank,
                "detail_net_amount": detail_net,
                "detail_rows": len(detail_rows),
                "bank_amount": bank_amount,
                "bank_date": join_unique(bridge_rows.get("bank_date", [])),
                "bank_tx_id": join_unique(bridge_rows.get("tx_id", [])),
                "detail_vs_bank_delta": detail_delta,
                "platform_vs_bank_delta": platform_delta,
                "status": status,
                "note": note,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def local_top_bank_candidates_for_doc(doc: pd.Series, bank: pd.DataFrame, limit: int = 3, window_days: int = 45) -> pd.DataFrame:
    if bank.empty:
        return bank.copy()
    rows = []
    signed_amount = float(doc.get("signed_amount") or 0)
    for _, tx in bank.iterrows():
        tx_amount = float(tx.get("amount") or 0)
        if tx_amount * signed_amount <= 0:
            continue
        day_diff = days_between(tx.get("date"), doc.get("date"))
        if day_diff > window_days:
            continue
        amount_diff = abs(tx_amount - signed_amount)
        score = (
            max(0, 1 - amount_diff / max(abs(tx_amount), 1)) * 0.45
            + max(0, 1 - day_diff / max(window_days, 1)) * 0.30
            + text_similarity(doc.get("text", ""), tx.get("text", "")) * 0.25
        )
        payload = tx.to_dict()
        payload["day_diff"] = day_diff
        payload["amount_diff"] = round(amount_diff, 2)
        payload["candidate_score"] = round(score, 4)
        rows.append(payload)
    return pd.DataFrame(rows).sort_values("candidate_score", ascending=False).head(limit) if rows else pd.DataFrame()


def local_candidate_docs_for_tx(tx: pd.Series, docs: pd.DataFrame, settings: MatchSettings) -> pd.DataFrame:
    if docs.empty:
        return docs.copy()
    rows = []
    tx_amount = float(tx.get("amount") or 0)
    target_abs = abs(tx_amount)
    for _, doc in docs.iterrows():
        signed_amount = float(doc.get("signed_amount") or 0)
        if tx_amount * signed_amount <= 0:
            continue
        day_diff = days_between(tx.get("date"), doc.get("date"))
        if day_diff > settings.batch_window_days:
            continue
        if abs(signed_amount) > max(target_abs + max(settings.max_fee_abs, target_abs * settings.max_fee_pct), 1.0):
            continue
        score = (
            text_similarity(doc.get("text", ""), tx.get("text", "")) * 0.52
            + max(0, 1 - day_diff / max(settings.batch_window_days, 1)) * 0.32
            + max(0, 1 - abs(abs(signed_amount) - target_abs) / max(target_abs, 1)) * 0.16
        )
        payload = doc.to_dict()
        payload["day_diff"] = day_diff
        payload["candidate_score"] = round(score, 4)
        rows.append(payload)
    return pd.DataFrame(rows).sort_values(["candidate_score", "date"], ascending=[False, True]).head(settings.max_batch_candidates) if rows else pd.DataFrame()


def days_between(a: object, b: object) -> int:
    if a is None or b is None or pd.isna(a) or pd.isna(b):
        return 99999
    return abs(int((pd.Timestamp(a).normalize() - pd.Timestamp(b).normalize()).days))


def build_ai_doc_candidates(open_docs: pd.DataFrame, open_bank: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, doc in open_docs.sort_values(["date", "signed_amount"]).iterrows():
        candidates = local_top_bank_candidates_for_doc(doc, open_bank, limit=3, window_days=45)
        if candidates.empty:
            rows.append(ai_doc_row(doc, None, "low", "No plausible FYRST single transaction in a 45-day window. Likely platform batch, missing platform export detail, or non-FYRST payment route."))
            continue
        for rank, (_, tx) in enumerate(candidates.iterrows(), start=1):
            assessment = assess_doc_candidate(doc, tx)
            rows.append(ai_doc_row(doc, tx, assessment["confidence"], assessment["note"], rank=rank))
    return pd.DataFrame(rows)


def build_ai_bank_candidates(open_bank: pd.DataFrame, open_docs: pd.DataFrame, settings: MatchSettings) -> pd.DataFrame:
    rows = []
    for _, tx in open_bank.sort_values(["date", "amount"]).iterrows():
        candidates = local_candidate_docs_for_tx(tx, open_docs, settings)
        if candidates.empty:
            rows.append(ai_bank_row(tx, None, "low", "No Accountable candidate fits amount/date direction. This bank line may be private, tax/interest, a pure transfer, or a missing Accountable document."))
            continue
        for rank, (_, doc) in enumerate(candidates.head(5).iterrows(), start=1):
            note = assess_bank_candidate(tx, doc)
            rows.append(ai_bank_row(tx, doc, note["confidence"], note["note"], rank=rank))
    return pd.DataFrame(rows)


def build_ai_summary(
    evidence: pd.DataFrame,
    bank_report: pd.DataFrame,
    payout_report: pd.DataFrame,
    ai_doc_candidates: pd.DataFrame,
    ai_bank_candidates: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    status_counts = evidence["evidence_level"].value_counts().to_dict() if not evidence.empty else {}
    rows.append(
        {
            "bereich": "Overall",
            "anzahl": len(evidence),
            "betrag": round(float(evidence["amount"].sum()), 2) if "amount" in evidence else 0.0,
            "ki_einschaetzung": f"Deterministic evidence levels: {status_counts}.",
            "naechster_schritt": "Review open and partial rows first; completed rows are audit trail candidates.",
            "beispiele": "",
        }
    )

    unresolved = evidence[evidence["evidence_level"].isin(["offen", "teilweise belegt"])].copy()
    for (level, doc_type, platform), group in unresolved.groupby(
        ["evidence_level", "doc_type", unresolved.apply(infer_platform_from_evidence, axis=1)],
        dropna=False,
    ):
        rows.append(
            {
                "bereich": f"Documents: {level} / {doc_type} / {platform or 'unknown'}",
                "anzahl": len(group),
                "betrag": round(float(group["amount"].sum()), 2),
                "ki_einschaetzung": unresolved_group_note(level, doc_type, platform, group),
                "naechster_schritt": unresolved_group_action(platform, group),
                "beispiele": join_unique(group["doc_ref"].head(5)),
            }
        )

    open_bank = bank_report[bank_report["Status"].eq("offen")].copy() if not bank_report.empty else pd.DataFrame()
    for platform, group in open_bank.groupby(open_bank.apply(infer_platform_from_bank, axis=1), dropna=False):
        rows.append(
            {
                "bereich": f"Open FYRST: {platform or 'unknown'}",
                "anzahl": len(group),
                "betrag": round(float(group["FYRST-Betrag"].sum()), 2) if "FYRST-Betrag" in group else 0.0,
                "ki_einschaetzung": "Open bank group after direct, PayPal and platform bridge matching.",
                "naechster_schritt": "Classify as transfer/private/tax or add the missing Accountable receipt if business-related.",
                "beispiele": join_unique(group["Verwendungszweck"].head(3)),
            }
        )

    if not payout_report.empty:
        for status, group in payout_report.groupby("status"):
            rows.append(
                {
                    "bereich": f"Platform payout reconciliation: {status}",
                    "anzahl": len(group),
                    "betrag": round(float(group["expected_bank_amount"].sum()), 2),
                    "ki_einschaetzung": payout_status_note(status),
                    "naechster_schritt": payout_status_action(status),
                    "beispiele": join_unique(group["payout_id"].head(5)),
                }
            )

    high_doc = ai_doc_candidates[ai_doc_candidates["ai_confidence"].eq("high")] if not ai_doc_candidates.empty else pd.DataFrame()
    high_bank = ai_bank_candidates[ai_bank_candidates["ai_confidence"].eq("high")] if not ai_bank_candidates.empty else pd.DataFrame()
    rows.append(
        {
            "bereich": "AI-style candidate review",
            "anzahl": len(high_doc) + len(high_bank),
            "betrag": 0,
            "ki_einschaetzung": "These are plausible but unconfirmed suggestions from local reasoning, not booked matches.",
            "naechster_schritt": "Use them as review hints; only confirmed rows should become hard matches.",
            "beispiele": "",
        }
    )
    return pd.DataFrame(rows)


def ai_doc_row(doc: pd.Series, tx: pd.Series | None, confidence: str, note: str, rank: int = 1) -> dict[str, object]:
    return {
        "doc_ref": doc.get("doc_ref", ""),
        "doc_date": doc.get("date"),
        "doc_amount": doc.get("signed_amount"),
        "doc_counterparty": doc.get("counterparty", ""),
        "doc_description": doc.get("description", ""),
        "rank": rank,
        "bank_date": "" if tx is None else tx.get("date"),
        "bank_amount": "" if tx is None else tx.get("amount"),
        "bank_counterparty": "" if tx is None else tx.get("counterparty", ""),
        "bank_description": "" if tx is None else tx.get("description", ""),
        "amount_diff": "" if tx is None else round(abs(float(tx.get("amount")) - float(doc.get("signed_amount"))), 2),
        "day_diff": "" if tx is None else abs(int((pd.Timestamp(tx.get("date")) - pd.Timestamp(doc.get("date"))).days)),
        "text_score": "" if tx is None else text_similarity(doc.get("text", ""), tx.get("text", "")),
        "ai_confidence": confidence,
        "ai_note": note,
    }


def ai_bank_row(tx: pd.Series, doc: pd.Series | None, confidence: str, note: str, rank: int = 1) -> dict[str, object]:
    return {
        "bank_date": tx.get("date"),
        "bank_amount": tx.get("amount"),
        "bank_counterparty": tx.get("counterparty", ""),
        "bank_description": tx.get("description", ""),
        "rank": rank,
        "doc_ref": "" if doc is None else doc.get("doc_ref", ""),
        "doc_date": "" if doc is None else doc.get("date"),
        "doc_amount": "" if doc is None else doc.get("signed_amount"),
        "doc_counterparty": "" if doc is None else doc.get("counterparty", ""),
        "doc_description": "" if doc is None else doc.get("description", ""),
        "candidate_score": "" if doc is None else doc.get("candidate_score", ""),
        "ai_confidence": confidence,
        "ai_note": note,
    }


def assess_doc_candidate(doc: pd.Series, tx: pd.Series) -> dict[str, str]:
    amount_diff = abs(float(tx.get("amount")) - float(doc.get("signed_amount")))
    day_diff = abs(int((pd.Timestamp(tx.get("date")) - pd.Timestamp(doc.get("date"))).days))
    sim = text_similarity(doc.get("text", ""), tx.get("text", ""))
    if amount_diff <= 0.05 and day_diff <= 7:
        return {"confidence": "high", "note": "Amount is exact and date window is tight; good manual-confirmation candidate."}
    if amount_diff <= max(1.0, abs(float(doc.get("signed_amount"))) * 0.05) and (day_diff <= 21 or sim >= 0.15):
        return {"confidence": "medium", "note": "Amount is close or text/date has overlap; check manually before accepting."}
    return {"confidence": "low", "note": "Weak candidate only; likely not enough for an automatic match."}


def assess_bank_candidate(tx: pd.Series, doc: pd.Series) -> dict[str, str]:
    score = float(doc.get("candidate_score") or 0)
    if score >= 0.75:
        return {"confidence": "high", "note": "Strong local candidate for this open bank line; still needs confirmation."}
    if score >= 0.45:
        return {"confidence": "medium", "note": "Possible relation, but not strong enough to book automatically."}
    return {"confidence": "low", "note": "Weak candidate."}


def infer_platform_from_evidence(row: pd.Series) -> str:
    text = " ".join(
        str(row.get(column) or "")
        for column in ["platform", "platforms", "counterparty", "description", "evidence_path", "evidence_note"]
    ).lower()
    return infer_platform(text)


def infer_platform_from_bank(row: pd.Series) -> str:
    text = " ".join(str(row.get(column) or "") for column in ["Plattform", "Gegenpartei", "Verwendungszweck", "Matchlogik"]).lower()
    return infer_platform(text)


def infer_platform(text: str) -> str:
    for platform in ["etsy", "paypal", "ebay", "shopify", "juniqe", "redbubble", "gelato", "printful", "amazon", "stripe"]:
        if platform in text:
            return platform
    return ""


def unresolved_group_note(level: str, doc_type: str, platform: str, group: pd.DataFrame) -> str:
    if level == "teilweise belegt":
        return "A supporting detail exists, but the final FYRST chain is not fully closed."
    if platform in {"etsy", "ebay", "shopify"} and doc_type == "income":
        return "Likely marketplace revenue where the bank side is a payout/batch rather than a single customer payment."
    if doc_type == "expense":
        return "Likely missing supplier/bank trail or a card/PayPal transaction not present in the uploaded FYRST scope."
    return "No stable support trail found in the uploaded data."


def unresolved_group_action(platform: str, group: pd.DataFrame) -> str:
    if platform == "etsy":
        return "Check whether the Accountable receipt uses gross sale, net sale, or fee amount; compare with Etsy payout reconciliation."
    if platform in {"ebay", "shopify"}:
        return "Upload payout/fee export for this platform if available; order exports alone support customer/order identity but not the bank bridge."
    if platform == "paypal":
        return "Check PayPal transaction type and related transfer; some rows may be fees/conversions without a matching invoice."
    return "Manually classify or add the missing receipt/export."


def payout_status_note(status: str) -> str:
    if status == "balanced":
        return "Platform detail, platform payout and FYRST transfer reconcile."
    if status == "bank_balanced_details_gap":
        return "Bank payout is proven, but linked order/fee details do not add to the same amount."
    if status == "bank_bridge_only":
        return "Bank payout is proven, but there is no linked order/fee detail."
    if status == "no_bank_bridge":
        return "Platform payout exists without a matching FYRST transfer in uploaded bank data."
    return "Needs manual review."


def payout_status_action(status: str) -> str:
    if status == "balanced":
        return "Use as strong evidence chain."
    if status == "bank_balanced_details_gap":
        return "Look for reserves, refunds, adjustments or sales outside the exported date range."
    if status == "bank_bridge_only":
        return "Upload the matching sales/payment detail export for that period."
    if status == "no_bank_bridge":
        return "Check if payout landed on another bank account or outside the FYRST export range."
    return "Open manually."


def join_unique(values) -> str:
    seen = []
    for value in values:
        if value is None or pd.isna(value):
            continue
        text = str(value).strip()
        if not text or text == "nan" or text in seen:
            continue
        seen.append(text)
    return " | ".join(seen)


if __name__ == "__main__":
    raise SystemExit(main())
