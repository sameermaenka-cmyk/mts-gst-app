import os, base64, io, json, re, time, logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import pdfplumber, anthropic, openpyxl
from openpyxl.styles import Font, PatternFill, Alignment

log = logging.getLogger(__name__)

# Cache: msg_id -> list of extracted PDF text strings
_pdf_text_cache = {}

MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]


def _retry(fn, description="operation"):
    """Retry fn() on transient errors (rate limits, server errors)."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except anthropic.RateLimitError as e:
            delay = RETRY_DELAYS[attempt]
            log.warning(f"{description}: rate limited, retrying in {delay}s (attempt {attempt+1})")
            time.sleep(delay)
        except anthropic.InternalServerError as e:
            delay = RETRY_DELAYS[attempt]
            log.warning(f"{description}: server error, retrying in {delay}s (attempt {attempt+1})")
            time.sleep(delay)
        except HttpError as e:
            if e.resp.status in (429, 500, 503):
                delay = RETRY_DELAYS[attempt]
                log.warning(f"{description}: Gmail {e.resp.status}, retrying in {delay}s (attempt {attempt+1})")
                time.sleep(delay)
            else:
                raise
    return fn()


SUPPLIER_QUERY = {
    'Ashgrove':       'from:ashgrovecheese.com.au subject:{inv}',
    'Bega':           'from:noreplyBDD@bega.com.au subject:09{inv}',
    'Bidfood':        'from:bidfood.com.au subject:I{inv}',
    'IFP':            'from:mailforalex.com subject:{inv}',
    'Freshline':      'from:freshline.net.au subject:{inv}',
    'Juicy Isle':     'from:juicyisle.com.au subject:{inv}',
    'Goodman Fielder':'from:goodmanfielder.com.au subject:09{inv}',
    'Lactalis':       'from:parmalat.com.au subject:{inv}',
    'News Corp':      'from:circulation.news.com.au has:attachment',
    'Pandani Select': 'from:pandaniselect.com.au subject:{inv}',
    'Savour Foods':   'from:savourfoods.com.au subject:{inv}',
    'Scottsdale Pork':'from:fresho.com subject:{inv}',
}

LABEL_QUERY = {
    'Ashgrove':       'label:Ashgrove-Cheese has:attachment',
    'Bega':           'label:Bega has:attachment',
    'Bidfood':        'label:Bidfood has:attachment',
    'IFP':            'label:Island-Fresh-Produce has:attachment',
    'Freshline':      'label:Freshline has:attachment',
    'Juicy Isle':     'label:Juicy-Isle has:attachment',
    'Goodman Fielder':'label:Goodman-Fielder has:attachment',
    'Lactalis':       'label:Lactalis has:attachment',
    'News Corp':      'label:News-Corp has:attachment',
    'Pandani Select': 'label:Pandani-Select has:attachment',
    'Savour Foods':   'label:Savour-Foods has:attachment',
    'Scottsdale Pork':'label:Fresho-Scottsdale has:attachment',
    'Tas Bakeries':   'label:Tasmanian-Bakeries-Natinal-Pies has:attachment',
    'Cartel & Co':    'label:Cartel-Co has:attachment',
    'PFD':            'label:PFD-Foods has:attachment',
    'Eden Foods':     'label:Eden-Foods has:attachment',
    'Olsen Eggs':     'label:Olsons-eggs has:attachment',
}

SUPPLIER_QUERY2 = {
    'Cripps Nu Bake':   'from:cripps.com.au has:attachment',
    'Wayside Butcher':  'from:wayside has:attachment',
    'Nichols Poultry':  'subject:{inv} has:attachment',
    'PFD':              'from:pfdfoods.com.au has:attachment',
    'Petuna Fisherie':  'from:petuna has:attachment',
    'Tasfresh':         'from:accounts.receivable@tasfresh.com.au subject:"Tasfresh AR Invoice for 30349 CAMPBELL TOWN" has:attachment',
    'Scottsdale Pork':  'from:fresho.com has:attachment',
    'Tas Bakeries':     'from:tasmanianbakeries.com.au has:attachment',
    'Sunrise Bakery':   'from:sunrise has:attachment',
    'Cartel & Co':      'from:cartelco.co has:attachment',
    'Licensed Socks':   'subject:{inv} has:attachment',
    'Horticultural L':  'subject:{inv} has:attachment',
    'Natures Foods':    'subject:{inv} has:attachment',
    'Olsen Eggs':       'subject:{inv} has:attachment',
    'Bega':             'from:noreplyBDD@bega.com.au has:attachment',
    'Bidfood':          'from:bidfood.com.au has:attachment',
    'Lactalis':         'from:parmalat.com.au has:attachment',
}


# Suppliers that only provide paper invoices (no email).
# These are skipped during Gmail search and marked PAPER INVOICE.
PAPER_SUPPLIERS = {
    'Wayside Butcher',
    'Tas Bakeries',
    'Sunrise Bakery',
    'Olsen Eggs',
    'Mountainvale',
    'Packings',
    'Tas Gift Wrap',
}

# Suppliers known to send weekly summary emails rather than per-invoice PDFs.
# For these, we require the invoice number to appear in the PDF text to avoid
# matching the summary (which has a fixed total unrelated to individual invoices).
SUMMARY_EMAIL_SUPPLIERS = {
    'Scottsdale Pork',
}

# Suppliers whose invoices arrive on EMAIL2 only.
# Skip email1 searches entirely for these to avoid wasting time.
EMAIL2_ONLY_SUPPLIERS = {
    'Tasfresh',
}


def get_api_key():
    """Get Anthropic API key from env or Streamlit secrets."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    try:
        import streamlit as st
        return st.secrets["ANTHROPIC_API_KEY"]
    except Exception:
        pass
    raise ValueError("ANTHROPIC_API_KEY not found in environment or Streamlit secrets")


def get_service(token_path):
    """Build Gmail API service from a token file path."""
    creds = Credentials.from_authorized_user_file(token_path)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)


def get_service_from_json(token_data):
    """Build Gmail API service from token JSON string or dict."""
    if isinstance(token_data, dict):
        info = dict(token_data)
    elif isinstance(token_data, str):
        # Strip wrapping quotes that Render/TOML may add
        s = token_data.strip().strip("'\"")
        info = json.loads(s)
    else:
        info = dict(token_data)
    creds = Credentials.from_authorized_user_info(info)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)


def get_services():
    """Get Gmail services from env vars, secrets, or local token files."""
    svc1, svc2 = None, None

    # Try env vars first (JSON strings)
    t1 = os.environ.get("TOKEN1")
    t2 = os.environ.get("TOKEN2")

    # Try Streamlit secrets
    if not t1:
        try:
            import streamlit as st
            t1 = st.secrets.get("TOKEN1")
            t2 = st.secrets.get("TOKEN2")
        except Exception:
            pass

    if t1:
        svc1 = get_service_from_json(t1)
        if t2:
            svc2 = get_service_from_json(t2)
        return svc1, svc2

    # Fall back to local token files
    token1_path = os.path.expanduser('~/Desktop/mts-gst-app/token.json')
    token2_path = os.path.expanduser('~/Desktop/mts-gst-app/token2.json')
    if os.path.exists(token1_path):
        svc1 = get_service(token1_path)
    if os.path.exists(token2_path):
        svc2 = get_service(token2_path)
    return svc1, svc2


def read_pdfs(svc, msg_id):
    """Extract text from PDF attachments in a Gmail message. Results are cached by msg_id."""
    if msg_id in _pdf_text_cache:
        return _pdf_text_cache[msg_id]

    texts = []
    try:
        msg = _retry(
            lambda: svc.users().messages().get(userId='me', id=msg_id, format='full').execute(),
            f"Gmail get message {msg_id}")

        def scan(parts):
            for p in parts:
                if p.get('parts'):
                    scan(p['parts'])
                if p.get('filename', '').lower().endswith('.pdf'):
                    aid = p.get('body', {}).get('attachmentId')
                    if aid:
                        att = _retry(
                            lambda aid=aid: svc.users().messages().attachments().get(
                                userId='me', messageId=msg_id, id=aid).execute(),
                            f"Gmail get attachment {msg_id}")
                        data = base64.urlsafe_b64decode(att['data'])
                        with pdfplumber.open(io.BytesIO(data)) as pdf:
                            t = '\n'.join(pg.extract_text() or '' for pg in pdf.pages)
                            if t.strip():
                                texts.append(t)
        scan(msg.get('payload', {}).get('parts', []))
    except (HttpError, Exception) as e:
        log.warning(f"read_pdfs({msg_id}): {type(e).__name__}: {e}")

    _pdf_text_cache[msg_id] = texts
    return texts


def extract_gst_and_total(text, client):
    """Use Claude Haiku to extract total and GST from invoice text."""
    def _call():
        return client.messages.create(
            model='claude-haiku-4-5-20251001', max_tokens=150,
            messages=[{'role': 'user', 'content':
                f'From this invoice extract TOTAL AMOUNT DUE and TOTAL GST INCLUDED. '
                f'Return ONLY JSON: {{"total":0.00,"gst":0.00}}\n\n{text[-2000:]}'}])

    r = _retry(_call, "Claude extract GST")
    t = re.sub(r'```json|```', '', r.content[0].text).strip()
    d = json.loads(t[t.find('{'):t.rfind('}') + 1])
    return d.get('total'), d.get('gst')


def search_and_verify(svc, query, tir_amount, inv_no, client, require_inv_in_text=False):
    """Search Gmail for an invoice and verify the amount matches TIR.

    If require_inv_in_text is True, the invoice number must appear in the PDF
    text for a match to be accepted. This prevents summary emails (with a fixed
    total) from being mistaken for individual invoices.
    """
    try:
        msgs = _retry(
            lambda: svc.users().messages().list(userId='me', q=query, maxResults=5).execute(),
            f"Gmail search '{query[:50]}'"
        ).get('messages', [])
    except Exception as e:
        log.warning(f"search_and_verify: Gmail search failed: {type(e).__name__}: {e}")
        return None, None, None

    for m in msgs:
        for txt in read_pdfs(svc, m['id']):
            if require_inv_in_text and inv_no not in txt:
                log.info(f"search_and_verify: skipping msg {m['id']} — inv {inv_no} not found in PDF text")
                continue
            try:
                t, g = extract_gst_and_total(txt, client)
                if t is not None and abs(abs(t) - abs(tir_amount)) < 1.00:
                    return g, t, 'VERIFIED ✓'
                elif t is not None:
                    return g, t, f'AMOUNT MISMATCH (PDF:{t} TIR:{tir_amount})'
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                log.warning(f"search_and_verify: parse error for msg {m['id']}: {e}")
            except Exception as e:
                log.warning(f"search_and_verify: unexpected error for msg {m['id']}: {type(e).__name__}: {e}")
    return None, None, None


def parse_tir_pdf(pdf_bytes, client):
    """Parse a TIR statement PDF into a list of (date, supplier, inv_no, amount) tuples."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = '\n'.join(pg.extract_text() or '' for pg in pdf.pages)

    def _call():
        return client.messages.create(
            model='claude-sonnet-4-20250514', max_tokens=8000,
            messages=[{'role': 'user', 'content':
                'Extract ALL invoice line items from this TIR statement. '
                'For each line return: date (DD/MM/YYYY), supplier name, invoice number, and amount (as a number, negative for credits). '
                'Return ONLY a JSON array of arrays: [["06/03/2026","Supplier Name","INV123",427.62], ...]\n'
                'Include every single line item. Do not skip any.\n\n'
                f'{full_text}'}])

    r = _retry(_call, "Claude parse TIR PDF")
    text = re.sub(r'```json|```', '', r.content[0].text).strip()
    data = json.loads(text[text.find('['):text.rfind(']') + 1])
    return [(row[0], row[1], str(row[2]), float(row[3])) for row in data]


def reconcile(tir_data, svc1, svc2, api_key, progress_callback=None):
    """Run reconciliation on TIR data. Returns list of result tuples.

    progress_callback(i, total, supplier, inv_no, status) is called after each invoice.
    """
    # Single client reused for all Claude calls
    client = anthropic.Anthropic(api_key=api_key)
    # Clear PDF text cache from any previous run
    _pdf_text_cache.clear()

    results = []
    total_count = len(tir_data)

    for i, (date, supplier, inv_no, tir_amount) in enumerate(tir_data):
        gst = None
        inv_total = None
        status = 'NOT FOUND'

        # Skip paper-based suppliers — no email invoices exist
        if supplier in PAPER_SUPPLIERS:
            status = 'PAPER INVOICE'
            results.append((date, supplier, inv_no, tir_amount, gst, inv_total, status))
            if progress_callback:
                progress_callback(i + 1, total_count, supplier, inv_no, status)
            continue

        # For summary-email suppliers, require invoice number in PDF text
        # to avoid matching weekly summary emails with fixed totals
        needs_inv_check = supplier in SUMMARY_EMAIL_SUPPLIERS

        # Skip email1 entirely for suppliers that only receive on email2
        if supplier not in EMAIL2_ONLY_SUPPLIERS:
            q_specific = SUPPLIER_QUERY.get(supplier, '').replace('{inv}', inv_no)
            queries_email1 = [q for q in [q_specific, f'subject:{inv_no} has:attachment -from:tir.com.au'] if q.strip()]

            # Try label query on email 1
            lq = LABEL_QUERY.get(supplier)
            if lq:
                g, t, s = search_and_verify(svc1, lq, tir_amount, inv_no, client,
                                             require_inv_in_text=needs_inv_check)
                if s and 'VERIFIED' in s:
                    gst, inv_total, status = g, t, 'VERIFIED ✓ (label)'
                elif s and gst is None:
                    gst, inv_total, status = g, t, s

            # Try email 1 specific queries
            for q in queries_email1:
                if 'VERIFIED' in status:
                    break
                g, t, s = search_and_verify(svc1, q, tir_amount, inv_no, client,
                                             require_inv_in_text=needs_inv_check)
                if s and 'VERIFIED' in s:
                    gst, inv_total, status = g, t, s
                elif s:
                    gst, inv_total, status = g, t, s

        # Try email 2 if not verified yet
        if 'VERIFIED' not in status and svc2:
            q2_tmpl = SUPPLIER_QUERY2.get(supplier, f'subject:{inv_no} has:attachment')
            q2_specific = q2_tmpl.replace('{inv}', inv_no)
            queries_email2 = [q2_specific, f'subject:{inv_no} has:attachment -from:tir.com.au']
            for q in queries_email2:
                g, t, s = search_and_verify(svc2, q, tir_amount, inv_no, client,
                                             require_inv_in_text=needs_inv_check)
                if s and 'VERIFIED' in s:
                    gst, inv_total, status = g, t, 'VERIFIED ✓ (email2)'
                    break
                elif s and 'MISMATCH' not in status:
                    gst, inv_total, status = g, t, f'MISMATCH email2 (PDF:{t} TIR:{tir_amount})'

        results.append((date, supplier, inv_no, tir_amount, gst, inv_total, status))

        if progress_callback:
            progress_callback(i + 1, total_count, supplier, inv_no, status)

    return results


def build_excel(results, title_date=''):
    """Build Excel workbook from results. Returns bytes."""
    total_count = len(results)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'GST Reconciliation'
    ws.merge_cells('A1:G1')
    ws['A1'] = f'IGA Campbell Town — TIR GST Reconciliation {title_date}'
    ws['A1'].font = Font(bold=True, size=13)
    ws['A1'].alignment = Alignment(horizontal='center')
    ws.merge_cells('A2:G2')
    ws['A2'] = 'GST extracted from supplier invoice PDFs — verified by matching TIR total amount'
    ws['A2'].alignment = Alignment(horizontal='center')
    ws.append([])
    ws.append(['Date', 'Supplier', 'Invoice No.', 'TIR Amount ($)', 'Invoice Total ($)', 'Actual GST ($)', 'Status'])
    for c in ws[4]:
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = PatternFill('solid', start_color='1a2d45')
        c.alignment = Alignment(horizontal='center')

    green = PatternFill('solid', start_color='E8F5E9')
    orange = PatternFill('solid', start_color='FFF3E0')
    red = PatternFill('solid', start_color='FFEBEE')
    grey = PatternFill('solid', start_color='F5F5F5')
    total_gst = 0
    verified_count = 0

    for date, supplier, inv_no, tir_amt, gst, inv_total, status in results:
        ws.append([date, supplier, inv_no, tir_amt, inv_total or '', gst or '', status])
        row = ws[ws.max_row]
        if 'VERIFIED' in status:
            for c in row:
                c.fill = green
            if gst:
                total_gst += gst
            verified_count += 1
        elif 'PAPER INVOICE' in status:
            for c in row:
                c.fill = grey
        elif 'MISMATCH' in status:
            for c in row:
                c.fill = orange
        else:
            for c in row:
                c.fill = red

    ws.append([])
    ws.append(['', '', 'TOTALS', sum(r[3] for r in results), '', round(total_gst, 2),
               f'{verified_count}/{total_count} verified'])
    for c in ws[ws.max_row]:
        c.font = Font(bold=True)
    for col, w in zip('ABCDEFG', [14, 20, 16, 16, 16, 16, 28]):
        ws.column_dimensions[col].width = w

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue(), verified_count, total_count, total_gst


# CLI entry point
if __name__ == '__main__':
    api_key = get_api_key()
    svc1, svc2 = get_services()

    # Hardcoded TIR for CLI testing — in production, use parse_tir_pdf()
    TIR = [
        ('06/03/2026','Ashgrove','723418',427.62),('06/03/2026','Ashgrove','724095',147.44),
        ('06/03/2026','Ashgrove','724704',558.80),('06/03/2026','Ashgrove','724705',293.13),
        ('06/03/2026','Bega','25904255',728.03),('06/03/2026','Bega','25911322',1165.66),
        ('06/03/2026','Bega','25918561',990.95),('06/03/2026','Bega','25934042',843.02),
        ('06/03/2026','Bega','77007488',-620.05),('06/03/2026','Bidfood','69543372',220.27),
        ('06/03/2026','Bidfood','C6974657',42.12),('06/03/2026','Cartel & Co','IOA86090',530.13),
        ('06/03/2026','Cripps Nu Bake','925206',2614.83),('06/03/2026','Cripps Nu Bake','926026',2676.54),
        ('06/03/2026','Eden Foods','RIN74955',2883.09),('06/03/2026','Eden Foods','RIN75028',74.79),
        ('06/03/2026','Eden Foods','RIN75153',115.74),('06/03/2026','Freshline','S338646',1641.11),
        ('06/03/2026','Freshline','S339355',2194.47),('06/03/2026','Freshline','S340009',2168.97),
        ('06/03/2026','Goodman Fielder','98633224',526.31),('06/03/2026','Horticultural L','241393-1',513.89),
        ('06/03/2026','IFP','1515901',-160.17),('06/03/2026','IFP','1516070',-64.57),
        ('06/03/2026','IFP','1516240',60.00),('06/03/2026','IFP','1516368',120.00),
        ('06/03/2026','IFP','1516451',-13.50),('06/03/2026','IFP','1516461',-22.50),
        ('06/03/2026','IFP','1516478',22.50),('06/03/2026','IFP','1516503',-40.56),
        ('06/03/2026','IFP','M261862',433.26),('06/03/2026','IFP','M262113',803.03),
        ('06/03/2026','IFP','M262249',524.47),('06/03/2026','IFP','M262476',2174.10),
        ('06/03/2026','IFP','M262656',1629.22),('06/03/2026','IFP','M262673',506.62),
        ('06/03/2026','IFP','M262703',176.70),('06/03/2026','IFP','M262812',1575.17),
        ('06/03/2026','IFP','M262834',167.05),('06/03/2026','IFP','M262839',544.47),
        ('06/03/2026','IFP','M262855',41.90),('06/03/2026','IFP','M263072',1630.77),
        ('06/03/2026','Juicy Isle','1444158',3314.60),('06/03/2026','Lactalis','42964661',181.18),
        ('06/03/2026','Lactalis','43063841',441.78),('06/03/2026','Licensed Socks','I0153649',653.40),
        ('06/03/2026','News Corp','8048633',549.08),('06/03/2026','Nichols Poultry','553981',487.27),
        ('06/03/2026','Nichols Poultry','554148',515.44),('06/03/2026','Pandani Select','378328',443.87),
        ('06/03/2026','Pandani Select','378477',249.26),('06/03/2026','Petuna Fisherie','20078404',143.88),
        ('06/03/2026','PFD','LT658333',506.05),('06/03/2026','PFD','LT670256',-39.71),
        ('06/03/2026','Savour Foods','SAV55775',493.26),('06/03/2026','Scottsdale Pork','50770801',694.46),
        ('06/03/2026','Scottsdale Pork','50861583',699.03),('06/03/2026','Sunrise Bakery','912070',222.05),
        ('06/03/2026','Tas Bakeries','NV118186',786.66),('06/03/2026','Tas Bakeries','NV118503',205.64),
        ('06/03/2026','Tasfresh','118664',-269.06),('06/03/2026','Tasfresh','2001709',279.10),
        ('06/03/2026','Tasfresh','2001714',50.00),('06/03/2026','Tasfresh','2001946',255.90),
        ('06/03/2026','Tasfresh','4526767',1061.28),('06/03/2026','Tasfresh','4526845',403.51),
        ('06/03/2026','Tasfresh','4527785',309.84),('06/03/2026','Tasfresh','4527940',1023.63),
        ('06/03/2026','Wayside Butcher','5735',2177.56),('06/03/2026','Wayside Butcher','5748',2389.72),
        ('13/03/2026','Ashgrove','725329',201.98),('13/03/2026','Ashgrove','726027',347.17),
        ('13/03/2026','Ashgrove','726936',653.63),('13/03/2026','Ashgrove','726937',404.27),
        ('13/03/2026','Bega','25949221',1014.21),('13/03/2026','Bega','25956542',918.53),
        ('13/03/2026','Bega','25956543',1714.83),('13/03/2026','Bega','25977037',475.17),
        ('13/03/2026','Bega','77036666',-935.44),('13/03/2026','Bidfood','69618480',726.82),
        ('13/03/2026','Cripps Nu Bake','926886',2903.39),('13/03/2026','Eden Foods','RIN75406',2780.54),
        ('13/03/2026','Freshline','S340947',1504.14),('13/03/2026','Goodman Fielder','98650450',539.82),
        ('13/03/2026','IFP','M263173',162.10),('13/03/2026','IFP','M263187',1067.99),
        ('13/03/2026','IFP','M263348',1149.39),('13/03/2026','IFP','M263496',1396.67),
        ('13/03/2026','IFP','M263663',2035.42),('13/03/2026','IFP','M263821',1261.29),
        ('13/03/2026','IFP','M264035',1494.77),('13/03/2026','Juicy Isle','1445371',7683.71),
        ('13/03/2026','Juicy Isle','1445498',614.89),('13/03/2026','Juicy Isle','R1445371',-745.20),
        ('13/03/2026','Momentum Foods','10071798',643.88),('13/03/2026','Natures Foods','24807',476.80),
        ('13/03/2026','News Corp','8056663',477.07),('13/03/2026','Nichols Poultry','554360',260.42),
        ('13/03/2026','Nichols Poultry','554490',166.14),('13/03/2026','Olsen Eggs','62514',414.00),
        ('13/03/2026','Olsen Eggs','62628',207.00),('13/03/2026','Pandani Select','378602',255.63),
        ('13/03/2026','Pandani Select','378760',291.68),('13/03/2026','Pandani Select','378854',346.88),
        ('13/03/2026','Petuna Fisherie','20078566',203.00),('13/03/2026','PFD','LT731916',1035.00),
        ('13/03/2026','Scottsdale Pork','50967307',653.86),('13/03/2026','Scottsdale Pork','51128792',721.04),
        ('13/03/2026','Sunrise Bakery','912209',177.55),('13/03/2026','Tas Bakeries','NV118736',426.47),
        ('13/03/2026','Tas Bakeries','NV119075',538.28),('13/03/2026','Tasfresh','2002177',259.35),
        ('13/03/2026','Tasfresh','2002387',270.25),('13/03/2026','Tasfresh','4529196',1032.76),
        ('13/03/2026','Tasfresh','4529197',286.88),('13/03/2026','Tasfresh','4529885',541.49),
        ('13/03/2026','Tasfresh','4529936',1422.92),('13/03/2026','Tasfresh','9185051',444.48),
        ('13/03/2026','Wayside Butcher','5764',1861.47),('13/03/2026','Wayside Butcher','5765',818.90),
        ('13/03/2026','Wayside Butcher','5781',2115.93),('13/03/2026','Wayside Butcher','5787',108.94),
    ]

    def cli_progress(i, total, supplier, inv_no, status):
        print(f'[{i}/{total}] {supplier} {inv_no} → {status}')

    results = reconcile(TIR, svc1, svc2, api_key, progress_callback=cli_progress)
    excel_bytes, verified, total, total_gst = build_excel(results, 'Week Ended 13/03/2026')

    out = os.path.expanduser('~/Desktop/MTS_GST_13Mar2026.xlsx')
    with open(out, 'wb') as f:
        f.write(excel_bytes)
    print(f'\n✅ DONE! Saved: {out}')
    print(f'Verified: {verified}/{total} | Total GST: ${total_gst:.2f}')
