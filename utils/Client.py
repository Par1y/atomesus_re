import asyncio
import warnings

from curl_cffi.requests import AsyncSession


class Client:
    def __init__(self, proxy=None, timeout=120, verify=True, impersonate='safari15_3'):
        self.proxies = {"http": proxy, "https": proxy}
        self.timeout = timeout
        self.verify = verify
        self.impersonate = impersonate
        self.session = AsyncSession(
            proxies=self.proxies,
            timeout=self.timeout,
            impersonate=self.impersonate,
            verify=self.verify,
        )
        self.session2 = AsyncSession(
            proxies=self.proxies,
            timeout=self.timeout,
            impersonate=self.impersonate,
            verify=self.verify,
        )

    async def post(self, *args, **kwargs):
        return await self.session.post(*args, **kwargs)

    async def post_stream(self, *args, headers=None, cookies=None, **kwargs):
        if self.session:
            headers = headers or self.session.headers
            cookies = cookies or self.session.cookies
        return await self.session2.post(*args, headers=headers, cookies=cookies, **kwargs)

    async def get(self, *args, **kwargs):
        return await self.session.get(*args, **kwargs)

    async def request(self, *args, **kwargs):
        return await self.session.request(*args, **kwargs)

    async def put(self, *args, **kwargs):
        return await self.session.put(*args, **kwargs)

    async def close(self):
        if hasattr(self, 'session'):
            try:
                await self.session.close()
                del self.session
            except Exception:
                pass
        if hasattr(self, 'session2'):
            try:
                await self.session2.close()
                del self.session2
            except Exception:
                pass
