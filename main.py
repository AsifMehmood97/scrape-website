from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
import logging
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
from xml_scraping import xml_process_links, extract_more_links
from site_scraping import site_process_links, fetch_url_text
import uvicorn
import pandas as pd
from mangum import Mangum

logging.basicConfig(filename='website_crawler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()
handler = Mangum(app)

async def handle_xml(all_text, processed_links, urls):
    
    responses = await xml_process_links(urls, processed_links)
    
    for text, link in responses:
        if text:
            all_text[link] = re.sub(r"\s+", " ", text)
    
    return all_text

async def handle_site(all_text, processed_links, urls, base_url):
    
    responses = await site_process_links(urls, processed_links, base_url)
    
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
async def extract_all_text(
    
    url: str = Body(
        ...,
        description="The site URL to be scrapped."
    ),
    
    processed_urls: list = Body(
        [],
        description="The site URLs that are already scrapped."
    ),
    
    all_urls: list = Body(
        [], 
        description="All discovered URLs till now."
    ), 
    
    sitemap: bool = Body(
        ...,
        description="Whether the sitemap exist for the website."
    ),
    
    url_only: bool = Body(
        False, 
        description="For scrapping only the provided URL. By default is 'False'."
    )
):
    
    if not url:
        return JSONResponse({"error": "Missing/Wrong URL in request body"}), 400

    all_text = {}
    base_url = urlparse(url).scheme + "://" + urlparse(url).netloc

    if url_only != False:
        urls = [url]
        all_text[url] = await fetch_url_text(url)
        return JSONResponse(all_text)

    try:

        if len(processed_urls) == 0:

            sitemap_url = url + "/sitemap.xml"
            sitemap_response = requests.get(sitemap_url)
            
            sitemap_links = []
            
            if sitemap_response.status_code == 200 and 'xml' in sitemap_response.headers.get('Content-Type'):
                
                sitemap_soup = BeautifulSoup(sitemap_response.content, "xml")
                
                for loc in sitemap_soup.find_all("loc"):
                    link = loc.text.strip()
                    if link not in processed_urls:
                        sitemap_links.append(link)

                urls = await extract_more_links(sitemap_links, processed_urls)
                
                all_text = await handle_xml(all_text, processed_urls, urls[0:5])
                
                return JSONResponse({
                    "scraped":all_text,
                    "processed_urls":list(all_text.keys()),
                    "all_urls":urls,
                    "sitemap":True
                    }
                )
            else:
                logging.info("Sitemap.xml not found or not valid XML for {}".format(url))
    
            urls = [url]
            
            all_text, urls = await handle_site(all_text, processed_urls, urls, base_url)
            
            urls_without_main = [i for i in urls if i != url]

            all_text, urls = await handle_site(all_text, processed_urls, urls_without_main[0:5], base_url)
            
            all_urls = list(pd.unique(urls))
            
            return JSONResponse({
                "scraped":all_text,
                "processed_urls":list(all_text.keys()),
                "all_urls":all_urls,
                "sitemap":False
                }
            )
        
        else:
            remaining_urls = [i for i in all_urls if i not in processed_urls]
            
            if sitemap == False:
                all_text, urls = await handle_site(all_text, processed_urls, remaining_urls[0:5], base_url) 
                all_urls.extend(urls)
                all_urls = list(pd.unique(all_urls))

                return JSONResponse({
                    "scraped":all_text,
                    "processed_urls":list(all_text.keys()),
                    "all_urls":all_urls,
                    "sitemap":False
                    }
                )
            
            else:
                all_text = await handle_xml(all_text, processed_urls, remaining_urls[0:5])
                
                return JSONResponse({
                    "scraped":all_text,
                    "processed_urls":list(all_text.keys()),
                    "all_urls":all_urls,
                    "sitemap":True
                    }
                )

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return JSONResponse({"error": f"Failed to fetch website: {e}"}), 500

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)