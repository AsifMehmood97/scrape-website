from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
import logging
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
from xml_scraping import xml_process_links
from site_scraping import site_process_links, fetch_url_text
import asyncio
from db_handler import urls_insert, check_site, extract_urls_insert, get_extracted_urls
import uvicorn
import nest_asyncio

logging.basicConfig(filename='website_crawler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

nest_asyncio.apply()

def handle_xml(all_text, processed_links, sitemap_links):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    responses = loop.run_until_complete(xml_process_links(sitemap_links, processed_links))
    loop.close()
    if responses is None:
        return all_text
    for text, link in responses:
        if text:
            all_text[link] = re.sub(r"\s+", " ", text)
    return all_text

def handle_site(all_text, processed_links, urls, base_url):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    responses = loop.run_until_complete(site_process_links(urls, processed_links, base_url))
    loop.close()
    urls = []
    for text, links, _, link in responses:
        if text:
            all_text[link] = re.sub(r"\s+", " ", text)
        urls.extend(links)
    return all_text, urls

@app.get("/")
async def index():
    return JSONResponse({"Status":True, "Message":"Scrapping API is up and running...."})

@app.post("/extract")
async def extract_all_text(url: str = Body(..., description="The site URL to be scrapped."), urls_list: list = Body([], description="The site URLs that are already scrapped."), url_only: bool = Body(False, description="Optional: For scrapping only the provided URL. By default is 'False'.")):
    
    if not url:
        return JSONResponse({"error": "Missing/Wrong URL in request body"}), 400
    
    if not url_only:
        url_only = False

    all_text = {}
    processed_links = set()
    base_url = urlparse(url).scheme + "://" + urlparse(url).netloc

    if url_only != False:
        urls = [url]
        all_text[url] = fetch_url_text(url)
        return JSONResponse(all_text)

    try:        
        
        if len(urls_list) != 0:
            for _url in urls_list:
                processed_links.add(_url)

        if check_site(url) is None:

            sitemap_url = url + "/sitemap.xml"
            sitemap_response = requests.get(sitemap_url)
            
            sitemap_links = []
            if sitemap_response.status_code == 200 and 'xml' in sitemap_response.headers.get('Content-Type'):
                sitemap_soup = BeautifulSoup(sitemap_response.content, "xml")
                for loc in sitemap_soup.find_all("loc"):
                    link = loc.text.strip()
                    if link not in processed_links:
                        sitemap_links.append(link)
                all_text = handle_xml(all_text, processed_links, sitemap_links)
                return JSONResponse(all_text)
            else:
                logging.info("Sitemap.xml not found or not valid XML for {}".format(url))
            
            url_id = urls_insert(url)
            urls = [url]
            all_text, urls = handle_site(all_text, processed_links, urls, base_url)
            extract_urls_insert(urls, url_id)
            all_text, urls = handle_site(all_text, processed_links, urls[0:5], base_url)
            extract_urls_insert(urls, url_id)
            return JSONResponse(all_text)
        else:
            url_id = check_site(url)[0]
            db_urls = get_extracted_urls(url_id)
            request_urls = []
            for ul in db_urls:
                if ul[0] not in processed_links:
                    request_urls.append(ul[0])
            all_text, urls = handle_site(all_text, processed_links, request_urls[0:5], base_url)
            extract_urls_insert(urls, url_id)
            return JSONResponse(all_text)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return JSONResponse({"error": f"Failed to fetch website: {e}"}), 500

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)