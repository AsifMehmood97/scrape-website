import pymysql

def get_connection():
	return pymysql.connect(host="localhost",user="developer", password="RpdwjF4m", database="microservice")

def urls_insert(url):
	con = get_connection()
	params = (url)
	query = """INSERT INTO urls(url) Values(%s)"""
	try:
		with con.cursor() as cur:
			cur.execute(query, params)
			last_id = cur.lastrowid
			return last_id
	except Exception as e:
		pass
	finally:
		con.commit()
		con.close()

def check_site(url):
	con = get_connection()
	try:
		query = f"Select id from urls where url = '{url}'"
		with con.cursor() as cur:
			cur.execute(query)
			return cur.fetchone()
	except Exception as e:
		pass
	finally:
		con.commit()
		con.close()		

def extract_urls_insert(urls_list, url_id):
	con = get_connection()
	try:
		for url in urls_list:
			params = (url_id,url)
			query = """INSERT IGNORE INTO extracted_urls(url_id, url) Values(%s, %s)"""
			with con.cursor() as cur:
				cur.execute(query, params)
	except Exception as e:
		pass
	finally:
		con.commit()
		con.close()

def get_extracted_urls(url_id):
	con = get_connection()
	try:
		query = f"""select url from extracted_urls where url_id = {url_id}"""
		with con.cursor() as cur:
			cur.execute(query)
			return cur.fetchall()
	except Exception as e:
		pass
	finally:
		con.commit()
		con.close()
