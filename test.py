import time
import requests
from urllib.parse import quote_plus

ZENODO_RECORDS_API = "https://zenodo.org/api/records"

EXTENSIONS = [
    ".qdpx", ".qdc",
    ".MQDA", ".MQBAC", ".MQTC", ".MQEX", ".MQMTR",
    ".MX24", ".MX24BAC", ".MC24", ".MEX24",
    ".MX22", ".MX20", ".MX18", ".MX12", ".MX11", ".MX5", ".MX4", ".MX3", ".MX2",
    ".M2K", ".LOA", ".SEA", ".MTR", ".MOD", ".MEX22",
    ".nvp", ".nvpx",
    ".atlasproj", ".hpr7",
    ".ppj", ".pprj", ".qlt", ".f4p", ".qpd"
]

def build_query(extensions, datasets_only=True) -> str:
    # Quote each extension for safety
    ext_or = " OR ".join([f'"{e}"' for e in extensions])
    q = f'files.key:({ext_or})'
    if datasets_only:
        q = f'type:dataset AND {q}'
    return q

def search_zenodo(query: str, page_size: int = 25, max_pages: int | None = None, access_token: str | None = None):
    """
    Generator yielding Zenodo records matching query.
    Notes:
      - Anonymous requests may be limited in page size (Zenodo has caps).
      - Use access_token (personal token) if you hit stricter limits.
    """
    headers = {}
    params = {"q": query, "size": page_size}

    if access_token:
        # Zenodo supports access_token as query param (commonly used in their examples)
        params["access_token"] = access_token

    next_url = ZENODO_RECORDS_API
    page = 0

    while next_url:
        page += 1
        if max_pages and page > max_pages:
            break

        r = requests.get(next_url, params=params, headers=headers, timeout=30)

        # Basic backoff for rate limiting / transient issues
        if r.status_code in (429, 500, 502, 503, 504):
            retry_after = int(r.headers.get("Retry-After", "3"))
            time.sleep(retry_after)
            continue

        r.raise_for_status()
        data = r.json()

        hits = data.get("hits", {}).get("hits", [])
        for rec in hits:
            yield rec

        # Zenodo uses a "links.next" for pagination
        next_url = data.get("links", {}).get("next")
        # After the first request, params are already encoded in next_url
        params = {}  # prevent double-appending query params

def main():
    query = build_query(EXTENSIONS, datasets_only=True)
    print("Query:")
    print(query)
    print()

    count = 0
    for rec in search_zenodo(query, page_size=25, max_pages=3):  # max_pages=None for full crawl
        count += 1
        title = rec.get("metadata", {}).get("title", "<no title>")
        rec_id = rec.get("id", "<no id>")
        files = [f.get("key") for f in rec.get("files", []) if "key" in f]

        print(f"{count:03d} | id={rec_id} | {title}")
        if files:
            # show only first few filenames to keep output readable
            print("      files:", ", ".join(files[:5]), ("..." if len(files) > 5 else ""))
        else:
            print("      files: <none listed>")
        print()

    print(f"Done. Printed {count} records.")

if __name__ == "__main__":
    main()