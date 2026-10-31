"""Use the project's platform + certifi trust without weakening TLS checks."""
from requests.adapters import HTTPAdapter
from core.utils.shared_ssl import get_shared_ssl_context


class PlatformTrustAdapter(HTTPAdapter):
    def __init__(self):
        self.context = get_shared_ssl_context()
        super().__init__(max_retries=0)

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        kwargs['ssl_context'] = self.context
        return super().init_poolmanager(connections, maxsize, block=block, **kwargs)

    def proxy_manager_for(self, proxy, **kwargs):
        kwargs['ssl_context'] = self.context
        return super().proxy_manager_for(proxy, **kwargs)

    def build_connection_pool_key_attributes(self, request, verify, cert=None):
        host, pool = super().build_connection_pool_key_attributes(request, verify, cert)
        pool['ssl_context'] = self.context
        return host, pool
