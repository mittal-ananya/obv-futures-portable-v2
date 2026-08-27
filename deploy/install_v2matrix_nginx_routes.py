from __future__ import annotations

from datetime import datetime
from pathlib import Path


NGINX_PATH = Path("/etc/nginx/sites-enabled/viveka-dashboard")


def proxy_block(path: str, port: int, *, exact: bool) -> str:
    operator = "=" if exact else "^~"
    return f"""
    location {operator} {path} {{
        proxy_pass http://127.0.0.1:{port};
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_cache off;
        proxy_redirect off;
    }}

"""


def main() -> None:
    text = NGINX_PATH.read_text(encoding="utf-8")
    required = ("/v2Matrix", "/api/v2matrix/", "/v2Matrix_portfolios", "/api/v2matrix-portfolios/")
    if all(item in text for item in required):
        print("v2Matrix routes already present")
        return

    backup = NGINX_PATH.with_name(
        f"{NGINX_PATH.name}.bak_v2matrix_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    backup.write_text(text, encoding="utf-8")
    block = (
        proxy_block("/v2Matrix", 8098, exact=True)
        + proxy_block("/v2Matrix/", 8098, exact=True)
        + proxy_block("/v2Matrix/v1", 8098, exact=True)
        + proxy_block("/v2Matrix/v2", 8098, exact=True)
        + proxy_block("/v2Matrix/notifications", 8098, exact=True)
        + proxy_block("/api/v2matrix/", 8098, exact=False)
        + proxy_block("/v2Matrix_portfolios", 8099, exact=True)
        + proxy_block("/v2Matrix_portfolios/", 8099, exact=True)
        + proxy_block("/api/v2matrix-portfolios/", 8099, exact=False)
    )
    marker = "    location = /Matrix {\n"
    if marker not in text:
        marker = "    location /dashboard {\n"
    if marker not in text:
        raise RuntimeError("route insertion marker not found")
    NGINX_PATH.write_text(text.replace(marker, block + marker, 1), encoding="utf-8")
    print(f"inserted v2Matrix routes; backup={backup}")


if __name__ == "__main__":
    main()
