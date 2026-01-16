#run_reviews.py


import sys, subprocess, importlib, os, time, json
from pathlib import Path


REQUIRED = [
    ("google-search-results", "serpapi"),
    ("pandas", "pandas"),
    ("python-dotenv", "dotenv"),
    ("tqdm", "tqdm"),
]

def ensure(pkg, import_name):
    try:
        return importlib.import_module(import_name)
    except ImportError:
        print(f"[setup] Installing {pkg} ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
        return importlib.import_module(import_name)

serpapi = ensure("google-search-results", "serpapi")
pd = ensure("pandas", "pandas")
dotenv = ensure("python-dotenv", "dotenv")
tqdm = ensure("tqdm", "tqdm")

from serpapi import GoogleSearch
from dotenv import load_dotenv
from tqdm import trange


def get_api_key():
    load_dotenv()  
    key = os.getenv("SERPAPI_KEY")
    if key:
        return key
   
    print("Insert SerpApi key:")
    key = input("SERPAPI_KEY = ").strip()
    if not key:
        sys.exit("Key empty. Restart")
    Path(".env").write_text(f"SERPAPI_KEY={key}\n", encoding="utf-8")
    print(f"[ok] Key saved to {Path('.env').resolve()}")
    return key

API_KEY = get_api_key()


def ask(prompt, default=None):
    s = input(f"{prompt}{' ['+str(default)+']' if default is not None else ''}: ").strip()
    return s if s else default

print("\nInsert park.")
print("data_id from URL Google Maps (from !1s...:0x... to !8m2).")
data_id = ask("data_id", default="0x47c60b883f0a74c7:0xe0f0efd82b7899e9")  # Nelson Mandela Park 
hl = ask("hl (en/nl)", default="en")
try:
    max_pages = int(ask("max_pages (how many pages)", default="10"))
except ValueError:
    max_pages = 10
out_path = ask("Name out CSV", default="reviews_output.csv")
try:
    pause = float(ask("Pause between queries", default="1.2"))
except ValueError:
    pause = 1.2

def ensure_parent_dir(path: str):
    d = Path(path).resolve().parent
    d.mkdir(parents=True, exist_ok=True)

def save_incremental(df: pd.DataFrame, save_path: str):

    ensure_parent_dir(save_path)
    if Path(save_path).exists():
        try:
            existing = pd.read_csv(save_path)
        except Exception:
            Path(save_path).rename(save_path + ".bak")
            existing = pd.DataFrame()
        combined = pd.concat([existing, df], ignore_index=True)
        if "review_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["review_id"]).reset_index(drop=True)
        combined.to_csv(save_path, index=False)
    else:
        if "review_id" in df.columns:
            df = df.drop_duplicates(subset=["review_id"]).reset_index(drop=True)
        df.to_csv(save_path, index=False)

def normalize_review(r: dict) -> dict:
    user = r.get("user") or {}
    return {
        "review_id": r.get("review_id"),
        "rating": r.get("rating"),
        "text": r.get("snippet") or r.get("text") or "",
        #"date_human": r.get("date"),
        "iso_date": r.get("iso_date"),
        "language": r.get("language"),
        #"author_name": user.get("name"),
        #"author_profile": user.get("link"),
        #"author_local_guide": user.get("local_guide"),
        #"author_reviews_count": user.get("reviews"),
        #"has_images": bool(r.get("images")),
    }

def fetch_one_page(api_key: str, data_id: str, hl: str, next_token: str | None):
    params = {
        "engine": "google_maps_reviews",
        "data_id": data_id,
        "hl": hl,
        "api_key": api_key,
    }
    if next_token:
        params["next_page_token"] = next_token
    return GoogleSearch(params).get_dict()


lockfile = out_path + ".lock"
if Path(lockfile).exists():
    sys.exit(f"Lock-файл {lockfile} already exist. Delete it to restart.")

Path(lockfile).write_text("locked", encoding="utf-8")

try:
    next_token = None
    got_any = False

    for i in trange(1, max_pages + 1, desc="Pages"):
        
        attempts, max_attempts, last_err = 0, 3, None
        while attempts < max_attempts:
            attempts += 1
            try:
                res = fetch_one_page(API_KEY, data_id, hl, next_token)
                break
            except Exception as e:
                last_err = e
                time.sleep(2 ** (attempts - 1))
        else:
            raise SystemExit(f"Error in page query {i}: {last_err}")

        
        if i == 1 and res.get("place_info"):
            print("\n[place_info]", json.dumps(res["place_info"], ensure_ascii=False, indent=2))

        reviews = res.get("reviews") or []
        if not reviews:
            print(f"\nPage {i}: no reviews, probably the end.")
            break

        rows = [normalize_review(r) for r in reviews]
        df_page = pd.DataFrame(rows)
        save_incremental(df_page, out_path)
        got_any = True

        pag = res.get("serpapi_pagination") or {}
        next_token = pag.get("next_page_token")
        if not next_token:
            print(f"\nPage {i}: next_page_token do not exist, no more pages.")
            break

        time.sleep(pause)

    if got_any:
       
        final_df = pd.read_csv(out_path)
        if "review_id" in final_df.columns:
            final_df = final_df.drop_duplicates(subset=["review_id"]).reset_index(drop=True)
            final_df.to_csv(out_path, index=False)
        print(f"\nREADY: {len(final_df)} lines saved → {out_path}")
    else:
        print("\nCouldn't get any reviews, check place_id and limits.")

finally:
   
    if Path(lockfile).exists():
        Path(lockfile).unlink()
