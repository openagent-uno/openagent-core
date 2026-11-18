"""A host-authorized, nonpersistent access grant for the shared protocol."""
from __future__ import annotations

import threading
import time


class AccessGrantManager:
    """No refresh material, credential discovery, file reads or token writes.

    The host authorizes one account and renews it before issuing the grant.
    Protocol admission uses the host's separate private admission path.
    """
    def __init__(self, auth, access_token: str, identifier: str, *, vendor_account_id='', plan=''):
        if not isinstance(access_token, str) or not access_token or not isinstance(identifier, str) or not identifier:
            raise ValueError('An access token and exact account identifier are required')
        self._auth = auth
        self._lock = threading.RLock()
        self._account = {'id':identifier, 'name':identifier, 'access_token':access_token,
                         'account_id':vendor_account_id, 'plan':plan, 'priority':0, 'enabled':True}

    def account_ids(self): return [self._account['id']]

    def list_accounts(self):
        with self._lock: return [dict(self._account)]

    def account_statuses(self):
        with self._lock:
            account = self._account
            return [{key:value for key,value in account.items() if key != 'access_token'} | {
                'managed':True, 'has_refresh_token':False,
                'limited':account.get('limited_until_ms', 0) > time.time() * 1000}]

    def select_account(self, *, exclude_ids=None, model=None):
        with self._lock:
            account = self._account
            if account['id'] in (exclude_ids or ()) or account.get('limited_until_ms', 0) > time.time() * 1000:
                raise self._auth.NoAvailableAccountError('The authorized subscription account is unavailable')
            fields = dict(id=account['id'], name=account['name'], priority=0, token=account['access_token'], managed=True)
            if self._auth.__name__.endswith('.codex.auth'):
                fields.update(account_id=account['account_id'], plan=account['plan'])
            return self._auth.AccountToken(**fields)

    def get_token(self): return self.select_account().token

    def get_account_token(self, identifier):
        if identifier != self._account['id']: raise KeyError('Unknown account')
        return self.get_token()

    def mark_rate_limited(self, account_id, cooldown_s, *, reason='', model=None):
        if account_id != self._account['id']: raise KeyError('Unknown account')
        with self._lock: self._account['limited_until_ms'] = time.time() * 1000 + max(0, cooldown_s) * 1000

    def mark_success(self, account_id, *, model=None):
        if account_id != self._account['id']: raise KeyError('Unknown account')

    def record_quota(self, account_id, quota):
        if account_id != self._account['id']: raise KeyError('Unknown account')
        with self._lock: self._account['quota'] = dict(quota)
