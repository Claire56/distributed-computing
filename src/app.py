
from dask.distributed import Client , LocalCluster
from bs4 import BeautifulSoup
import requests, os , time , pandas
import concurrent.futures as cf
import dask.dataframe as dd

def scrape_page(url):
    try:
        html = requests.get(url, timeout=10).text
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text().split()
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return []  # Return empty list on failure


# Parallel processing with multi-processes
def main1(url_file):
    start = time.perf_counter()
    # urls = pandas.read_csv(url_file)
    # urls = urls["url"].to_list()
    # Read CSV lazily (doesn't load into memory immediately)
    ddf = dd.read_csv(url_file)
    urls = ddf["url"].compute().tolist()  # Converts to a list

    with cf.ProcessPoolExecutor(max_workers=os.cpu_count()) as exc:
        results = list(exc.map(scrape_page, urls))

    print(results[:10])
    finish = time.perf_counter()
    print(f'\nFinished in {round(finish - start, 2)} second(s)\n')

# Parallel processing with Dask
def main(url_file):
    start = time.perf_counter()
    # urls = pandas.read_csv(url_file)
    # urls = urls["url"].to_list()

    # Read CSV lazily (doesn't load into memory immediately)
    ddf = dd.read_csv(url_file)
    urls = ddf["url"].compute().tolist()  # Converts to a list

    # Connect to the Dask cluster
    # client = Client("tcp://10.0.0.209:8786")  # Replace with your scheduler's address for multiple computers

    cluster = LocalCluster(n_workers=os.cpu_count())
    print(f'\nThis PC has  {os.cpu_count()} cores\n')
    client = Client(cluster)

    print(f"\n\nDask Dashboard: {client.dashboard_link}")  # Monitor progress here

    # Distribute the scraping tasks
    futures = client.map(scrape_page, urls)
    results = client.gather(futures)

    print(results[:10])
    finish = time.perf_counter()
    print(f'Dast Finished in {round(finish - start, 2)} second(s)')

    client.close()  # Shut down workers


if __name__ == '__main__':
    main("src/urls.csv")
    main1("src/urls.csv")

