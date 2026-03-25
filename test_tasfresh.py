#!/usr/bin/env python3
"""Test Tasfresh invoice matching against Gmail Email 2.

Connects to Email 2 (office.igacampbelltown@gmail.com) using TOKEN2,
searches for Tasfresh emails, lists attachment filenames, and tests
the regex matching against TIR invoice numbers.
"""
import json, re, sys, os

# Add project to path
sys.path.insert(0, os.path.dirname(__file__))

from run import (
    get_services, get_api_key, _refresh_creds, _filename_matches_inv,
    SUPPLIER_QUERY2, EMAIL2_ONLY_SUPPLIERS, ATTACHMENT_MATCH_SUPPLIERS,
)
from googleapiclient.errors import HttpError

# TIR Tasfresh invoice numbers from test data
TIR_TASFRESH = [
    ('118664', -269.06), ('2001709', 279.10), ('2001714', 50.00),
    ('2001946', 255.90), ('4526767', 1061.28), ('4526845', 403.51),
    ('4527785', 309.84), ('4527940', 1023.63),
    ('2002177', 259.35), ('2002387', 270.25), ('4529196', 1032.76),
    ('4529197', 286.88), ('4529885', 541.49), ('4529936', 1422.92),
    ('9185051', 444.48),
]


def main():
    print("=" * 60)
    print("TASFRESH MATCHING TEST")
    print("=" * 60)

    # Step 1: Check config
    print("\n--- Config Check ---")
    print(f"  EMAIL2_ONLY: {'Tasfresh' in EMAIL2_ONLY_SUPPLIERS}")
    print(f"  ATTACHMENT_MATCH: {'Tasfresh' in ATTACHMENT_MATCH_SUPPLIERS}")
    query = SUPPLIER_QUERY2.get('Tasfresh', 'NOT SET')
    print(f"  SUPPLIER_QUERY2: {query}")

    # Step 2: Connect to Email 2
    print("\n--- Connecting to Gmail ---")
    try:
        svc1, svc2 = get_services()
        if not svc2:
            print("  ERROR: svc2 (Email 2) is None — TOKEN2 not configured or refresh failed")
            return
        print(f"  svc1: {'OK' if svc1 else 'None'}")
        print(f"  svc2: OK")
    except Exception as e:
        print(f"  ERROR connecting: {type(e).__name__}: {e}")
        return

    # Step 3: Search Email 2 for Tasfresh
    print(f"\n--- Searching Email 2: {query} ---")
    try:
        results = svc2.users().messages().list(
            userId='me', q=query, maxResults=30
        ).execute()
        msgs = results.get('messages', [])
        print(f"  Found {len(msgs)} messages")
    except HttpError as e:
        print(f"  Gmail API error: {e.resp.status} {e._get_reason()}")
        return
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        return

    if not msgs:
        print("\n  NO MESSAGES FOUND. Trying broader queries...")
        for alt_q in [
            'from:accounts.receivable@tasfresh.com.au',
            'from:tasfresh.com.au',
            'subject:"Tasfresh AR Invoice"',
            'subject:tasfresh has:attachment',
            'tasfresh',
        ]:
            try:
                r = svc2.users().messages().list(userId='me', q=alt_q, maxResults=3).execute()
                n = r.get('resultSizeEstimate', 0)
                m = r.get('messages', [])
                print(f"  '{alt_q}' → {len(m)} messages (est: {n})")
                if m:
                    # Show first message details
                    msg = svc2.users().messages().get(
                        userId='me', id=m[0]['id'], format='metadata',
                        metadataHeaders=['From', 'Subject', 'Date']
                    ).execute()
                    headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}
                    print(f"    First: From={headers.get('From', '?')} Subject={headers.get('Subject', '?')}")
            except Exception as e:
                print(f"  '{alt_q}' → ERROR: {e}")
        return

    # Step 4: Examine each message — show headers and attachment filenames
    print("\n--- Message Details ---")
    all_filenames = []
    for i, m in enumerate(msgs[:30]):
        msg_id = m['id']
        try:
            msg = svc2.users().messages().get(userId='me', id=msg_id, format='full').execute()
        except Exception as e:
            print(f"  msg {i}: ERROR fetching: {e}")
            continue

        # Headers
        headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}
        print(f"\n  Message {i+1}: {msg_id}")
        print(f"    From:    {headers.get('From', '?')}")
        print(f"    To:      {headers.get('To', '?')}")
        print(f"    Subject: {headers.get('Subject', '?')}")
        print(f"    Date:    {headers.get('Date', '?')}")

        # Scan attachments
        filenames = []

        def scan(parts):
            for p in parts:
                if p.get('parts'):
                    scan(p['parts'])
                fname = p.get('filename', '')
                if fname:
                    filenames.append(fname)

        payload = msg.get('payload', {})
        if payload.get('parts'):
            scan(payload['parts'])
        if payload.get('filename'):
            filenames.append(payload['filename'])

        print(f"    Attachments ({len(filenames)}):")
        for f in filenames:
            print(f"      - {f}")
            all_filenames.append(f)

    # Step 5: Test filename matching against TIR invoice numbers
    print("\n--- Filename Matching Test ---")
    matched = 0
    unmatched = 0
    for inv_no, amount in TIR_TASFRESH:
        matches = [f for f in all_filenames if _filename_matches_inv(f, inv_no)]
        if matches:
            print(f"  MATCH: inv={inv_no} (${amount:,.2f}) → {matches[0]}")
            matched += 1
        else:
            print(f"  MISS:  inv={inv_no} (${amount:,.2f}) — no matching filename")
            unmatched += 1

    print(f"\n--- Summary ---")
    print(f"  Matched: {matched}/{len(TIR_TASFRESH)}")
    print(f"  Unmatched: {unmatched}/{len(TIR_TASFRESH)}")
    print(f"  Total attachments found: {len(all_filenames)}")


if __name__ == '__main__':
    main()
