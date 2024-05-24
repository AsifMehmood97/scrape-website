from fastapi import FastAPI, Body, HTTPException, status
from fastapi.responses import JSONResponse
from xml_scraping import xml_process_links
import logging
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from site_scraping import site_process_links
import re
import uvicorn

logging.basicConfig(filename='website_crawler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

def chunk_urls(urls_dict, chunk_size=5):
    urls_chunks = []
    chunk = {}
    count = 0
    for url, text in urls_dict.items():
        chunk[url] = text
        count += 1
        if count % chunk_size == 0:
            urls_chunks.append(chunk)
            chunk = {}
    if chunk:
        urls_chunks.append(chunk)
    return urls_chunks

@app.get("/")
async def index():
    return JSONResponse({"Status":True, "Message":"Scrapping API is up and running...."})

@app.post("/extract")
async def extract_all_text(url: str = Body(..., description="The site URL to be scrapped."), url_only: bool = Body(False, description="Optional: For scrapping only the provided URL. By default is 'False'.")):

    if not url:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing/Wrong URL in request body")
    
    print("url_only",url_only)
    
    try:
        processed_links = set()
        all_text = {}

        if url_only == False:
            sitemap_url = url + "/sitemap.xml"
            sitemap_response = requests.get(sitemap_url)  

            sitemap_links = []
            if sitemap_response.status_code == 200 and 'xml' in sitemap_response.headers.get('Content-Type'):
                sitemap_soup = BeautifulSoup(sitemap_response.content, "xml")
                for loc in sitemap_soup.find_all("loc"):
                    link = loc.text.strip()
                    if link not in processed_links:
                        sitemap_links.append(link)
                
                responses = await xml_process_links(sitemap_links, processed_links)
                for text, link in responses:
                    if text:
                        all_text[link] = re.sub(r"\s+", " ", text)
                all_text = chunk_urls(all_text)

                return JSONResponse(all_text)
            else:
                logging.info("Sitemap.xml not found or not valid XML for {}".format(url))

        base_url = urlparse(url).scheme + "://" + urlparse(url).netloc
        urls = [url]
        print("urls",urls)
        while urls:
            responses = await site_process_links(urls, processed_links, base_url)
            urls = []
            for text, links, _, link in responses:
                if text:
                    all_text[link] = re.sub(r"\s+", " ", text)
                urls.extend(links)
        all_text = chunk_urls(all_text)
        return JSONResponse(all_text)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return JSONResponse({"error": f"Failed to fetch website: {e}"}), 500
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
