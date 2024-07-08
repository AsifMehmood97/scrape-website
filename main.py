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

async def getUniqueURLs(urls):

    unique_urls = []

    for i in urls:
        if i not in unique_urls:
            unique_urls.append(i)
    
    return unique_urls


@app.get("/")
async def index():
    print("Index is called.")
    logging.info("Index is called...")
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
    
    print("API is called.")
    logging.info("API is called...")
    
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

            print("Checking sitemap.")
            logging.info("Checking sitemap...")

            sitemap_url = url + "/sitemap.xml"

            print("About to call request.")
            sitemap_response = requests.get(sitemap_url)
            print("Got request result.")
            
            sitemap_links = []
            
            if sitemap_response.status_code == 200 and 'xml' in sitemap_response.headers.get('Content-Type'):
                
                print("about BeautifulSoup.")
                sitemap_soup = BeautifulSoup(sitemap_response.content, "xml")
                print("BeautifulSoup.")
                for loc in sitemap_soup.find_all("loc"):
                    link = loc.text.strip()
                    if link not in processed_urls:
                        sitemap_links.append(link)
                print("about extract_more_links.")
                urls = await extract_more_links(sitemap_links, processed_urls)
                print("extract_more_links.")
                all_text = await handle_xml(all_text, processed_urls, urls[0:5])
                
                print("Creating JSON.")
                logging.info("Creating JSON...")
                
                response = {
                    "scraped":all_text,
                    "processed_urls":list(all_text.keys()),
                    "all_urls":urls,
                    "sitemap":True
                }
                

                print("response:",response)
                logging.info(response)
                return response

            else:
                print("Sitemap not found.")
                logging.info("Sitemap not found...")
                logging.info("Sitemap.xml not found or not valid XML for {}".format(url))
    
            urls = [url]
            
            all_text, urls = await handle_site(all_text, processed_urls, urls, base_url)
            
            urls_without_main = [i for i in urls if i != url]

            all_text, urls = await handle_site(all_text, processed_urls, urls_without_main[0:5], base_url)
            
            all_urls = getUniqueURLs(urls)
            
            print("Creating JSON.")
            logging.info("Creating JSON...")
            
            response = {
                "scraped":all_text,
                "processed_urls":list(all_text.keys()),
                "all_urls":all_urls,
                "sitemap":False
                }
            

            print("response:",response)
            logging.info(response)
            return response
        
        else:
            remaining_urls = [i for i in all_urls if i not in processed_urls]
            
            if sitemap == False:
                all_text, urls = await handle_site(all_text, processed_urls, remaining_urls[0:5], base_url) 
                all_urls.extend(urls)
                all_urls = getUniqueURLs(all_urls)
                print("Creating JSON.")
                logging.info("Creating JSON...")
                response = {
                    "scraped":all_text,
                    "processed_urls":list(all_text.keys()),
                    "all_urls":all_urls,
                    "sitemap":False
                    }
                

                print("response:",response)
                logging.info(response)
                return response
            
            else:
                all_text = await handle_xml(all_text, processed_urls, remaining_urls[0:5])
                print("Creating JSON.")
                logging.info("Creating JSON...")
                response = {
                    "scraped":all_text,
                    "processed_urls":list(all_text.keys()),
                    "all_urls":all_urls,
                    "sitemap":True
                    }
                
            
                print("response:",response)
                logging.info(response)
                return response

    except Exception as e:
        print("Error.")
        print(str(e))
        logging.error(f"Error: {e}")
        return JSONResponse({"error": f"Failed: {e}"})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)