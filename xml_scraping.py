import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
from pathlib import Path

async def xml_fetch_url(url, processed_links):
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

async def create_chunks(array):
    new_array = []
    for i in range(0, len(array), 5):
        new_array.append(array[i:i + 5])
    return new_array

async def xml_process_links(links, processed_links):
    if len([link for link in links if link.endswith('.xml')]) > 0:
        tasks = []
        for link in links:
            task = asyncio.create_task(fetch_inner_links(link))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)

        links = []
        for response in responses:
            for res in response:
                if res not in processed_links:
                    link_path = Path(res)
                    if link_path.suffix.lower() in ['.gif', '.mov','.png', '.jpg', '.jpeg', '.svg']:
                        continue
                    links.append(res)
    
    tasks = []

    chunked_array = await create_chunks(links)
    for chunk in chunked_array:
        for link in chunk:
            task = asyncio.create_task(xml_fetch_url(link, processed_links))
            tasks.append(task)
        
        return await asyncio.gather(*tasks)

    return None
