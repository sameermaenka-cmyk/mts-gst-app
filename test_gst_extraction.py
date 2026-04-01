"""Test GST extraction for 5 suppliers against Gmail (01/03/2026 – 20/03/2026).

Downloads actual PDFs from Gmail, runs the regex extractor, and prints:
  supplier | invoice number | total | GST | raw text near GST field
"""

import base64, io, re
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import pdfplumber
from run import _extract_with_regex

# ---------------------------------------------------------------------------
# Gmail helpers
# ---------------------------------------------------------------------------

def get_svc(token_path):
    creds = Credentials.from_authorized_user_file(token_path)
    if creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)


def search(svc, q, n=15):
    return svc.users().messages().list(
        userId='me', q=q, maxResults=n
    ).execute().get('messages', [])


def get_msg(svc, mid):
    return svc.users().messages().get(userId='me', id=mid, format='full').execute()


def extract_pdfs(svc, mid):
    """Return list of (filename, text) tuples for every PDF attachment."""
    msg = get_msg(svc, mid)
    results = []

    def scan(parts):
        for p in parts:
            if p.get('parts'):
                scan(p['parts'])
            fname = p.get('filename', '')
            if not fname:
                continue
            lower = fname.lower()
            if lower.endswith('.pdf') or lower.endswith('.p'):
                aid = p.get('body', {}).get('attachmentId')
                if aid:
                    att = svc.users().messages().attachments().get(
                        userId='me', messageId=mid, id=aid).execute()
                    data = base64.urlsafe_b64decode(att['data'])
                    try:
                        with pdfplumber.open(io.BytesIO(data)) as pdf:
                            txt = '\n'.join(pg.extract_text() or '' for pg in pdf.pages)
                        results.append((fname, txt))
                    except Exception as e:
                        results.append((fname, f'[PARSE ERROR: {e}]'))

    payload = msg.get('payload', {})
    scan(payload.get('parts', []))
    # single-part emails
    if not results:
        fname = payload.get('filename', '')
        if fname:
            aid = payload.get('body', {}).get('attachmentId')
            if aid:
                att = svc.users().messages().attachments().get(
                    userId='me', messageId=mid, id=aid).execute()
                data = base64.urlsafe_b64decode(att['data'])
                try:
                    with pdfplumber.open(io.BytesIO(data)) as pdf:
                        txt = '\n'.join(pg.extract_text() or '' for pg in pdf.pages)
                    results.append((fname, txt))
                except Exception as e:
                    results.append((fname, f'[PARSE ERROR: {e}]'))
    return results, msg


def header(msg, name):
    for h in msg['payload']['headers']:
        if h['name'] == name:
            return h['value']
    return '?'


def gst_context(text, supplier):
    """Return ~200 chars of raw text around the GST / total area."""
    # Find the most relevant anchor for each supplier
    patterns = {
        'Tasfresh':      r'\$[\d,]+\.\d{2}\s*\n.*?EOW',
        'Cripps Nu Bake': r'INVOICE TOTAL',
        'Tas Bakeries':  r'GST Total',
        'News Corp':     r'Taxable Supplies',
        'IFP':           r'Total excluding GST',
        'Freshline':     r'Total excluding GST',
    }
    pat = patterns.get(supplier, r'GST|Total')
    m = re.search(pat, text, re.DOTALL)
    if m:
        start = max(0, m.start() - 40)
        end = min(len(text), m.end() + 160)
        snippet = text[start:end].replace('\n', '\\n')
        return snippet
    # fallback: last 200 chars
    return text[-200:].replace('\n', '\\n')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

DATE_FILTER = 'after:2026/3/1 before:2026/3/21'

SUPPLIERS = [
    # (name, gmail_account, query)
    ('Tasfresh',      'token2.json',
     f'from:accounts.receivable@tasfresh.com.au has:attachment {DATE_FILTER}'),

    ('Cripps Nu Bake', 'token2.json',
     f'from:administration@cripps.com.au subject:"Cripps Invoice" has:attachment {DATE_FILTER}'),

    ('Tas Bakeries',  'token.json',
     f'from:tasmanianbakeries.com.au has:attachment {DATE_FILTER}'),

    ('News Corp',     'token.json',
     f'from:circulation.news.com.au has:attachment {DATE_FILTER}'),

    ('IFP',           'token.json',
     f'from:mailforalex.com has:attachment {DATE_FILTER}'),
]

# cache services
_svcs = {}


def svc_for(token_path):
    if token_path not in _svcs:
        _svcs[token_path] = get_svc(token_path)
    return _svcs[token_path]


def run():
    for supplier, token, query in SUPPLIERS:
        svc = svc_for(token)
        msgs = search(svc, query)

        print(f'\n{"="*90}')
        print(f'  {supplier}  ({len(msgs)} emails in date range)')
        print(f'{"="*90}')

        if not msgs:
            print('  ** No emails found **')
            continue

        for m in msgs:
            pdfs, msg = extract_pdfs(svc, m['id'])
            subj = header(msg, 'Subject')
            date = header(msg, 'Date')

            for fname, txt in pdfs:
                if '[PARSE ERROR' in txt:
                    print(f'  {fname}: {txt}')
                    continue

                # Extract invoice number from text or filename
                inv_no = '?'
                if supplier == 'Tasfresh':
                    # filename like InvoiceLAU4536869B.pdf → 4536869
                    fm = re.search(r'(\d{5,})', fname)
                    inv_no = fm.group(1) if fm else '?'
                elif supplier == 'Cripps Nu Bake':
                    fm = re.search(r'(\d{6,})', fname)
                    inv_no = fm.group(1) if fm else '?'
                elif supplier == 'Tas Bakeries':
                    im = re.search(r'Invoice Number:\s*(INV\d+)', txt)
                    inv_no = im.group(1) if im else '?'
                elif supplier == 'News Corp':
                    im = re.search(r'Invoice Number\s+(\d+)', txt)
                    inv_no = im.group(1) if im else '?'
                elif supplier == 'IFP':
                    im = re.search(r'Invoice Number\s+(\S+)', txt)
                    if not im:
                        im = re.search(r'Invoice[_\s]*(\S+?)\.PDF', fname, re.IGNORECASE)
                    inv_no = im.group(1) if im else '?'

                total, gst = _extract_with_regex(txt, supplier)

                ctx = gst_context(txt, supplier)

                status = 'OK' if total is not None else 'NO REGEX MATCH'
                gst_str = f'${gst:.2f}' if gst is not None else 'None'
                total_str = f'${total:.2f}' if total is not None else 'None'

                print(f'\n  Invoice: {inv_no:16s} | Total: {total_str:>10s} | '
                      f'GST: {gst_str:>8s} | {status}')
                print(f'  File:    {fname}')
                print(f'  Email:   {subj[:60]}  ({date[:20]})')
                print(f'  Context: {ctx[:200]}')


if __name__ == '__main__':
    run()
