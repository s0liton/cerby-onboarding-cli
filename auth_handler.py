import time

import jwt
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def _transient_evaluate_error(exc: BaseException) -> bool:
    """``page.evaluate`` fails while the document is navigating; retry shortly after."""
    msg = str(exc).lower()
    return "execution context was destroyed" in msg


def is_access_token_valid(token: str, *, skew_seconds: int = 60) -> bool:
    """True if JWT decodes and ``exp`` is in the future (with skew)."""
    if not token or not str(token).strip():
        return False
    try:
        payload = jwt.decode(token, options={"verify_signature": False})
        exp = payload.get("exp")
        if exp and exp > int(time.time()) + skew_seconds:
            return True
    except Exception:
        pass
    return False


class CerbyAuthHandler:
    """Opens Cerby in a browser; the user signs in with their IdP. Token is read from localStorage."""

    def __init__(self, workspace_slug: str):
        self.cerby_url = f"https://{workspace_slug}.cerby.com"
        self.access_token: str | None = None

    def _poll_token(self, page, timeout_s: float = 600.0) -> str:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                token = page.evaluate("window.localStorage.getItem('access_token')")
            except PlaywrightError as e:
                if _transient_evaluate_error(e):
                    time.sleep(0.5)
                    continue
                raise
            except Exception as e:
                if _transient_evaluate_error(e):
                    time.sleep(0.5)
                    continue
                raise
            if token and is_access_token_valid(token):
                return token
            time.sleep(2)
        raise TimeoutError(
            "Timed out waiting for access_token in localStorage after login."
        )

    def _login_in_browser(self, page) -> None:
        page.goto(self.cerby_url, timeout=60000)
        print(
            "\nSign in using the browser window (Okta, Microsoft, Cerby, etc.). "
            "When you reach the Cerby app and the session is ready, this tool will continue.\n"
        )

    def _login_and_get_token(self) -> None:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            try:
                self._login_in_browser(page)
                token = self._poll_token(page)
                if not token or not is_access_token_valid(token):
                    raise RuntimeError("Access token missing or invalid after login.")
                self.access_token = token
            except PlaywrightTimeoutError as e:
                raise RuntimeError(f"Login timed out: {e}") from e
            finally:
                context.close()
                browser.close()

    def get_access_token(self) -> str:
        if self.access_token and is_access_token_valid(self.access_token):
            return self.access_token
        self._login_and_get_token()
        if not self.access_token:
            raise RuntimeError("Login did not produce an access token.")
        return self.access_token
