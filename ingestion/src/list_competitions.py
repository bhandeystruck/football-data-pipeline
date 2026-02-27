from .api_client import get_json

def main():
    payload = get_json("/competitions")
    comps = payload.get("competitions", [])

    print("ID | CODE | NAME")
    print("-" * 60)
    for c in comps:
        cid = c.get("id")
        code = c.get("code")
        name = c.get("name")
        print(f"{cid} | {code} | {name}")

if __name__ == "__main__":
    main()