import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
from pathlib import Path

async def fetch_url(url, processed_links):
    async with aiohttp.ClientSession() as session:
        if url in processed_links:
            return None, url
        processed_links.add(url)
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                
                html = await response.text()

                soup = BeautifulSoup(html, "lxml")

                
                for tag in soup.find_all(["script", "style", "img", "canvas"]):
                    tag.decompose()

                for a_tag in soup.find_all("a", href=True):
                    parent = a_tag.parent
                    if parent.name in ["img", "canvas"]:
                        a_tag.decompose()

                text = soup.get_text(separator="\n").strip()

                return text, url
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching URL {url}: {e}")
            return None, url

async def fetch_inner_links(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                html = await response.text()
                available_links = []
                
                sitemap_soup = BeautifulSoup(html, "xml")
                
                for loc in sitemap_soup.find_all("loc"):
                    link = loc.text.strip()
                    available_links.append(link)
                return available_links
        except Exception as e:
            logging.error(f"Error fetching URL {url}: {e}")

async def xml_process_links(links, processed_links):
    if len([link for link in links if link.endswith('.xml')]) > 0:
        tasks = []
        for link in links:
            task = asyncio.create_task(fetch_inner_links(link))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)

        links = []
        for response in responses:
            links.extend(response)
    
        
    tasks = []
    for link in links:
        link_path = Path(link)
        if link_path.suffix.lower() in ['.gif', '.mov','.png', '.jpg', '.jpeg', '.svg']:
            continue
        task = asyncio.create_task(fetch_url(link, processed_links))
        tasks.append(task)
    responses = await asyncio.gather(*tasks)
    
    return responses
