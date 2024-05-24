import aiohttp
import asyncio
from bs4 import BeautifulSoup
import logging

async def fetch_url(url, processed_links, base_url):
    if url in processed_links:
        return "", set(), processed_links, url

    processed_links.add(url)
    logging.info(f"Processing URL: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()

                html = await response.text()

                soup = BeautifulSoup(html, "lxml")

                for tag in soup.find_all(["script", "style", "img", "canvas"]):
                    tag.decompose()

                text = soup.get_text(separator="\n").strip()

                links = set()
                for a_tag in soup.find_all("a", href=True):
                    link = a_tag["href"]
                    if link.startswith("http") and base_url in link:
                        links.add(link)
                    elif link.startswith("/"):
                        if link != "/":
                            links.add(f"{base_url}{link}")

                return text, links, processed_links, url
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching URL {url}: {e}")
    except Exception as e:
        logging.error(f"Unhandled exception in fetch_url for URL {url}: {e}")
    return "", set(), processed_links, url

async def site_process_links(urls, processed_links, base_url):
    tasks = []
    for url in urls:
        task = asyncio.create_task(fetch_url(url, processed_links, base_url))
        tasks.append(task)
    responses = await asyncio.gather(*tasks)
    return responses