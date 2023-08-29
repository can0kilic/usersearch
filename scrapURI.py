import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

MAX_THREADS = 20

def fetch_uri(href):
    endpoint = f"https://boris.unibe.ch/cgi/exportview/contributors_bern/{href}/JSON/{href}.js"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        uris = [item.get("uri") for item in data]
        return uris
    else:
        return []

def main():
    input_csv = "resultDbMain.csv"
    output_csv = "uri.csv"

    df = pd.read_csv(input_csv)
    href_list = df["href"].tolist()

    unique_uris = set()
    uri_text_mapping = {}  # New dictionary to store uri -> text mapping

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(fetch_uri, href.replace(".html", "")) for href in href_list]

        for future, row in zip(tqdm(as_completed(futures), total=len(futures), desc="Fetching URIs"), df.itertuples(index=False)):
            uris = future.result()
            unique_uris.update(uris)
            for uri in uris:
                uri_text_mapping[uri] = row.text  # Store text for each uri

    uri_df = pd.DataFrame({"uri": list(unique_uris), "text": [uri_text_mapping.get(uri, '') for uri in unique_uris]})
    uri_df.to_csv(output_csv, index=False)
    print(f"Unique URIs along with text saved to {output_csv}")

if __name__ == "__main__":
    main()
