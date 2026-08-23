from __future__ import annotations

from pathlib import Path


NGINX_PATH = Path("/etc/nginx/sites-enabled/viveka-dashboard")


def proxy_block(path: str, *, exact: bool) -> str:
    operator = "=" if exact else "^~"
    return f"""
    location {operator} {path} {{
        proxy_pass http://127.0.0.1:8096;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_redirect off;
    }}

"""


def main() -> None:
    text = NGINX_PATH.read_text()
    if "/dashboard/OBVFUTPORT/v2" in text and "/api/obvfutport/v2/" in text:
        print("routes already present")
        return

    backup = NGINX_PATH.with_name(NGINX_PATH.name + ".bak_obvfutport_v2")
    backup.write_text(text)
    block = proxy_block("/dashboard/OBVFUTPORT/v2", exact=True) + proxy_block("/api/obvfutport/v2/", exact=False)
    marker = "    location = /dashboard/OBVFUTPORT/v1 {\n"
    if marker not in text:
        marker = "    location /dashboard {\n"
    if marker not in text:
        raise RuntimeError("dashboard marker not found")
    NGINX_PATH.write_text(text.replace(marker, block + marker, 1))
    print(f"inserted OBVFUTPORT v2 routes; backup={backup}")


if __name__ == "__main__":
    main()
