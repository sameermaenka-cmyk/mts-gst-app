"""Test Cripps dedup logic with all edge cases."""
import re

def _norm_supplier(s):
    return re.sub(r'\s+', '', s).lower()

SCENARIOS = [
    ("Scenario 1: No dupes (hardcoded TIR)", [
        ('06/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('06/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps Nu Bake', '926886', 2903.39),
    ]),
    ("Scenario 2: Same inv + same amount", [
        ('06/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('06/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('13/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps Nu Bake', '926886', 2903.39),
    ]),
    ("Scenario 3: Same inv, DIFFERENT amount", [
        ('06/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('06/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps Nu Bake', '925206', 2622.22),
        ('13/03/2026', 'Cripps Nu Bake', '926026', 2679.87),
        ('13/03/2026', 'Cripps Nu Bake', '926886', 2903.39),
    ]),
    ("Scenario 4: Supplier name variation", [
        ('06/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('06/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps NuBake', '925206', 2614.83),
        ('13/03/2026', 'Cripps NuBake', '926026', 2676.54),
        ('13/03/2026', 'Cripps Nu Bake', '926886', 2903.39),
    ]),
    ("Scenario 5: Inv whitespace/prefix variation", [
        ('06/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('06/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps Nu Bake', ' 925206', 2614.83),
        ('13/03/2026', 'Cripps Nu Bake', '0925206', 2614.83),
        ('13/03/2026', 'Cripps Nu Bake', '926886', 2903.39),
    ]),
    ("Scenario 6: Name + amount variation combo", [
        ('06/03/2026', 'Cripps Nu Bake', '925206', 2614.83),
        ('06/03/2026', 'Cripps Nu Bake', '926026', 2676.54),
        ('13/03/2026', 'Cripps  Nu  Bake', '925206', 2622.22),  # extra spaces + diff amt
        ('13/03/2026', 'CRIPPS NU BAKE', '926026', 2679.87),    # all caps + diff amt
        ('13/03/2026', 'Cripps Nu Bake', '926886', 2903.39),
    ]),
]

all_pass = True
for label, tir_data in SCENARIOS:
    _seen_inv = set()
    _seen_amt = set()
    deduped = []
    for row in tir_data:
        norm_sup = _norm_supplier(row[1])
        inv_no = row[2].strip()
        amount = row[3]
        key_inv = (norm_sup, inv_no)
        key_amt = (norm_sup, amount)
        if key_inv in _seen_inv or key_amt in _seen_amt:
            continue
        _seen_inv.add(key_inv)
        _seen_amt.add(key_amt)
        deduped.append(row)

    cripps_before = [r for r in tir_data if 'cripps' in r[1].lower()]
    cripps_after = [r for r in deduped if 'cripps' in r[1].lower()]

    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    print(f"  BEFORE ({len(cripps_before)} rows):")
    for d, s, inv, amt in cripps_before:
        print(f"    {d} | {s:20s} | inv={repr(inv):12s} | amt={amt}")
    print(f"  AFTER ({len(cripps_after)} rows):")
    for d, s, inv, amt in cripps_after:
        print(f"    {d} | {s:20s} | inv={repr(inv):12s} | amt={amt}")

    w2_invs = {r[2].strip() for r in cripps_after if r[0] == '13/03/2026'}
    ok = '925206' not in w2_invs and '926026' not in w2_invs
    print(f"  Week 2 inv_nos: {sorted(w2_invs)}")
    print(f"  PASS: {'YES ✓' if ok else 'NO ✗'}")
    if not ok:
        all_pass = False

print(f"\n{'='*70}")
print(f"  ALL SCENARIOS: {'PASS ✓' if all_pass else 'FAIL ✗'}")
print(f"{'='*70}")
