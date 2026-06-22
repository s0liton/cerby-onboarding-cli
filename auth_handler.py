import queue
import threading
import time

import jwt
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

import token_session


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


def access_token_seconds_remaining(token: str) -> float | None:
    """Seconds until JWT ``exp``, or ``None`` if missing or not decodable."""
    if not token or not str(token).strip():
        return None
    try:
        payload = jwt.decode(token, options={"verify_signature": False})
        exp = payload.get("exp")
        if exp is None:
            return None
        return float(exp) - time.time()
    except Exception:
        return None


def poll_token_until_valid(
    page,
    timeout_s: float = 600.0,
    *,
    poll_interval_s: float = 2.0,
) -> str:
    """Read ``access_token`` from ``localStorage`` until it looks like a valid JWT."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            token = page.evaluate("() => window.localStorage.getItem('access_token')")
        except PlaywrightError as e:
            if _transient_evaluate_error(e):
                time.sleep(min(0.5, poll_interval_s))
                continue
            raise
        except Exception as e:
            if _transient_evaluate_error(e):
                time.sleep(min(0.5, poll_interval_s))
                continue
            raise
        if token and is_access_token_valid(str(token)):
            return str(token)
        time.sleep(poll_interval_s)
    raise TimeoutError(
        "Timed out waiting for access_token in localStorage after login."
    )


class CerbyAuthHandler:
    """Opens Cerby in a browser; the user signs in with their IdP. Token is read from localStorage."""

    def __init__(self, workspace_slug: str):
        self.cerby_url = f"https://{workspace_slug}.cerby.com"
        self.access_token: str | None = None

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
                token = poll_token_until_valid(page, timeout_s=600.0, poll_interval_s=2.0)
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


class BrowserTokenRefreshSession:
    """TEMPORARY / experimental: keep Chromium open, reload periodically, persist new ``access_token``.

    All Playwright usage runs on a single background thread (Playwright is not thread-safe).
    """

    def __init__(self, workspace_slug: str, *, refresh_interval_s: float = 60.0):
        self.workspace = workspace_slug.strip()
        self.refresh_interval_s = refresh_interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._first: queue.Queue[str | BaseException] = queue.Queue(maxsize=1)

    def start_and_wait_first_token(self, *, first_token_timeout_s: float = 600.0) -> str:
        self._thread = threading.Thread(
            target=self._worker,
            name="CerbyExpTokenBrowser",
            daemon=True,
        )
        self._thread.start()
        try:
            item = self._first.get(timeout=first_token_timeout_s)
        except queue.Empty as e:
            self._stop.set()
            if self._thread.is_alive():
                self._thread.join(timeout=5.0)
            raise RuntimeError(
                "Timed out waiting for the browser thread to produce an access token."
            ) from e
        if isinstance(item, BaseException):
            raise item
        return str(item)

    def stop(self, *, join_timeout_s: float = 20.0) -> None:
        self._stop.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=join_timeout_s)

    def _worker(self) -> None:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context()
                page = context.new_page()
                try:
                    cerby_url = f"https://{self.workspace}.cerby.com"
                    page.goto(cerby_url, timeout=60000)
                    print(
                        "\nSign in using the browser window (Okta, Microsoft, Cerby, etc.). "
                        "When you reach the Cerby app and the session is ready, this tool will continue.\n"
                        "[experimental] This window will stay open; the page reloads about every minute "
                        "to capture rotated access tokens to disk.\n",
                        flush=True,
                    )
                    token = poll_token_until_valid(page, timeout_s=600.0, poll_interval_s=2.0)
                    if not token or not is_access_token_valid(token):
                        raise RuntimeError("Access token missing or invalid after login.")
                    token_session.save_session(self.workspace, token)
                    self._first.put(token)

                    last_persisted = token
                    while not self._stop.is_set():
                        if self._stop.wait(timeout=self.refresh_interval_s):
                            break
                        try:
                            page.reload(wait_until="domcontentloaded", timeout=120000)
                        except PlaywrightTimeoutError as e:
                            print(
                                f"[experimental] page.reload timed out (will retry): {e}",
                                flush=True,
                            )
                            continue
                        except PlaywrightError as e:
                            print(
                                f"[experimental] page.reload failed (will retry): {e}",
                                flush=True,
                            )
                            continue
                        try:
                            new_tok = poll_token_until_valid(
                                page, timeout_s=45.0, poll_interval_s=0.4
                            )
                        except TimeoutError:
                            print(
                                "[experimental] No valid access_token in localStorage after reload; "
                                "will retry on next interval.",
                                flush=True,
                            )
                            continue
                        if new_tok != last_persisted:
                            token_session.save_session(self.workspace, new_tok)
                            last_persisted = new_tok
                            print(
                                "[experimental] Persisted a new access_token from the browser.",
                                flush=True,
                            )
                finally:
                    context.close()
                    browser.close()
        except BaseException as e:
            try:
                self._first.put_nowait(e)
            except queue.Full:
                pass
