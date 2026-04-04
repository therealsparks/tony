import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
SECRETS_PATH = BASE_DIR / '.openclaw/secrets/quickbooks.json'
OUTPUT_PATH = BASE_DIR / 'data/quickbooks/purchases.json'
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_LOOKBACK_DAYS = 400  # ~13 months


def load_secrets():
    data = json.loads(SECRETS_PATH.read_text())
    required = ['client_id', 'client_secret', 'realm_id', 'refresh_token']
    for key in required:
        if not data.get(key):
            raise RuntimeError(f'Missing {key} in QuickBooks secrets')
    return data


def refresh_access_token(secrets):
    url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
    auth = (secrets['client_id'], secrets['client_secret'])
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': secrets['refresh_token'],
    }
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    resp = requests.post(url, data=data, auth=auth, headers=headers, timeout=30)
    if not resp.ok:
        raise RuntimeError(f'Token refresh failed: {resp.status_code} {resp.text}')
    payload = resp.json()
    return payload['access_token'], payload.get('refresh_token') or secrets['refresh_token']


def fetch_purchases(access_token, realm_id, start_date, page_size=200):
    url = f'https://quickbooks.api.intuit.com/v3/company/{realm_id}/query'
    params = {'minorversion': '73'}
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/text',
        'Accept': 'application/json'
    }
    all_rows = []
    start_pos = 1
    while True:
        query = (
            "select Id, TxnDate, TotalAmt, CurrencyRef, AccountRef, PaymentType, PrivateNote, EntityRef,"
            " MetaData, Line from Purchase where TxnDate >= '{start_date}' order by TxnDate desc STARTPOSITION {start} MAXRESULTS {max_results}"
            .format(start_date=start_date, start=start_pos, max_results=page_size)
        )
        resp = requests.post(url, params=params, data=query, headers=headers, timeout=30)
        if not resp.ok:
            raise RuntimeError(f'Purchase query failed: {resp.status_code} {resp.text}')
        payload = resp.json()
        rows = payload.get('QueryResponse', {}).get('Purchase', [])
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        start_pos += page_size
    return {'QueryResponse': {'Purchase': all_rows}}


def transform(raw):
    query_response = raw.get('QueryResponse', {})
    purchases = query_response.get('Purchase', [])
    results = []
    for item in purchases:
        line_items = []
        for line in item.get('Line', []) or []:
            detail = line.get('AccountBasedExpenseLineDetail') or line.get('ItemBasedExpenseLineDetail') or {}
            line_items.append({
                'description': line.get('Description'),
                'amount': line.get('Amount'),
                'account': (detail.get('AccountRef') or {}).get('name') or (detail.get('AccountRef') or {}).get('value'),
                'class': (detail.get('ClassRef') or {}).get('name'),
                'customer': (detail.get('CustomerRef') or {}).get('name'),
            })
        results.append({
            'id': item.get('Id'),
            'doc_number': item.get('DocNumber'),
            'txn_date': item.get('TxnDate'),
            'total': item.get('TotalAmt'),
            'currency': (item.get('CurrencyRef') or {}).get('value'),
            'account': item.get('AccountRef'),
            'payment_type': item.get('PaymentType'),
            'vendor': (item.get('EntityRef') or {}).get('name') or (item.get('EntityRef') or {}).get('value'),
            'memo': item.get('PrivateNote'),
            'meta': item.get('MetaData'),
            'line_items': line_items,
        })
    return results


def save_output(purchases):
    payload = {
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'count': len(purchases),
        'purchases': purchases,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2))
    print(f'Wrote {len(purchases)} purchases to {OUTPUT_PATH}')


def update_refresh_token(secrets, new_refresh):
    if new_refresh and new_refresh != secrets['refresh_token']:
        secrets['refresh_token'] = new_refresh
        SECRETS_PATH.write_text(json.dumps(secrets, indent=2))
        print('Updated refresh_token in secrets file.')


def main():
    parser = argparse.ArgumentParser(description='Fetch QuickBooks purchases (expenses)')
    parser.add_argument('--access-token', help='Use an existing access token (skip refresh)')
    parser.add_argument('--days', type=int, default=DEFAULT_LOOKBACK_DAYS, help='Lookback window (days)')
    args = parser.parse_args()

    secrets = load_secrets()
    if args.access_token:
        print('Using provided access token (skipping refresh).')
        access_token = args.access_token
    else:
        access_token, new_refresh = refresh_access_token(secrets)
        if new_refresh:
            update_refresh_token(secrets, new_refresh)

    start_date = (datetime.now(timezone.utc) - timedelta(days=args.days)).date().isoformat()
    raw = fetch_purchases(access_token, secrets['realm_id'], start_date)
    purchases = transform(raw)
    save_output(purchases)


if __name__ == '__main__':
    main()
