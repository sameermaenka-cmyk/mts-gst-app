"""Parse ACTUAL TIR PDFs and test Cripps dedup with the real data."""
import io, re, json, os
import pdfplumber
import anthropic

TIR_PDFS = [
    os.path.expanduser('~/Downloads/349 Statement-079273.pdf'),
    os.path.expanduser('~/Downloads/349 Statement-079452.pdf'),
]

def parse_tir(pdf_path, client):
    with open(pdf_path, 'rb') as f:
        pdf_bytes = f.read()
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = '\n'.join(pg.extract_text() or '' for pg in pdf.pages)
    r = client.messages.create(
        model='claude-sonnet-4-20250514', max_tokens=8000,
        messages=[{'role': 'user', 'content':
            'Extract ALL invoice line items from this TIR statement. '
            'For each line return: date (DD/MM/YYYY), supplier name, invoice number, and amount (as a number, negative for credits). '
            'Return ONLY a JSON array of arrays: [["06/03/2026","Supplier Name","INV123",427.62], ...]\n'
            'Include every single line item. Do not skip any.\n\n'
            f'{full_text}'}])
    text = re.sub(r'```json|```', '', r.content[0].text).strip()
    data = json.loads(text[text.find('['):text.rfind(']') + 1])
    return [(row[0], row[1], str(row[2]), float(row[3])) for row in data]


def run_dedup(tir_data):
    """Run the NEW dedup logic (credit+replacement pair removal)."""
    def _norm_supplier(s):
        return re.sub(r'\s+', '', s).lower()

    # Phase 1: Identify mixed-sign invoice groups
    _inv_amounts = {}
    for row in tir_data:
        key = (_norm_supplier(row[1]), row[2].strip())
        _inv_amounts.setdefault(key, []).append(row[3])

    _mixed_sign_keys = set()
    for key, amounts in _inv_amounts.items():
        has_pos = any(a > 0 for a in amounts)
        has_neg = any(a < 0 for a in amounts)
        if has_pos and has_neg:
            _mixed_sign_keys.add(key)

    # Phase 2: Filter + deduplicate
    _seen_inv = set()
    deduped = []
    for row in tir_data:
        norm_sup = _norm_supplier(row[1])
        inv_no = row[2].strip()
        key = (norm_sup, inv_no)
        if key in _mixed_sign_keys:
            continue
        if key in _seen_inv:
            continue
        _seen_inv.add(key)
        deduped.append(row)
    return deduped


def analyze(tir_data, label):
    cripps_before = [r for r in tir_data if 'cripps' in r[1].lower()]

    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")
    print(f"  Cripps BEFORE dedup ({len(cripps_before)} rows):")
    for d, s, inv, amt in cripps_before:
        sign = "CREDIT" if amt < 0 else "      "
        print(f"    {d} | inv={inv:8s} | amt={amt:>10.2f} | {sign}")

    deduped = run_dedup(tir_data)
    cripps_after = [r for r in deduped if 'cripps' in r[1].lower()]

    print(f"\n  Cripps AFTER dedup ({len(cripps_after)} rows):")
    for d, s, inv, amt in cripps_after:
        print(f"    {d} | inv={inv:8s} | amt={amt:>10.2f}")

    # Check each week
    weeks = sorted(set(r[0] for r in cripps_after))
    for w in weeks:
        invs = [r[2] for r in cripps_after if r[0] == w]
        print(f"  {w}: {invs}")

    # Verify 925206/926026 do NOT appear alongside 926886
    w2_invs = set()
    for r in cripps_after:
        if '926886' in [rr[2] for rr in cripps_after if rr[0] == r[0]]:
            w2_invs.add(r[2])
    # The week containing 926886 should NOT contain 925206 or 926026
    ok = '925206' not in w2_invs and '926026' not in w2_invs
    print(f"\n  Week with 926886 also contains: {sorted(w2_invs)}")
    print(f"  PASS: {'YES ✓' if ok else 'NO ✗ — 925206/926026 in same week as 926886!'}")
    return ok


# --- Run ---
from run import get_api_key
client = anthropic.Anthropic(api_key=get_api_key())

all_pass = True
for pdf_path in TIR_PDFS:
    if not os.path.exists(pdf_path):
        print(f"NOT FOUND: {pdf_path}")
        continue
    print(f"\nParsing: {os.path.basename(pdf_path)} ...")
    tir_data = parse_tir(pdf_path, client)
    ok = analyze(tir_data, os.path.basename(pdf_path))
    if not ok:
        all_pass = False

print(f"\n{'='*80}")
print(f"  FINAL RESULT: {'ALL PASS ✓' if all_pass else 'FAIL ✗'}")
print(f"{'='*80}")
