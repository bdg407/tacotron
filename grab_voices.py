from bs4 import BeautifulSoup as bs
import pandas as pd
from tqdm import tqdm

import requests, urllib, uuid, os, sys
import multiprocessing as mp

target_path = '../glados_voices'
base_url = 'https://glados.biringa.com'

def process(link_url, link_title, dirname):
    if 'download.php' in link_url:
        try:
            hash_value = link_url.split('=')[1]
            filename = '%s/%s.wav' % (dirname, hash_value)
            filepath = os.path.join(target_path, filename)

            urllib.request.urlretrieve(base_url + link_url, filepath)
        except:
            print('failed %s - %s' % (dirname, link_url))
            return False

        return {'file': filename, 'title': link_title, 'url': link_url, 'hash': hash_value}

    return False

n = 0
def cb(x):
    global n, df, df_all

    if x is not False:
        df = df.append(x, ignore_index=True)
        df_all = df_all.append(x, ignore_index=True)

    n += 1
    sys.stdout.write('\r%s/%s' % (n, '?'))
    sys.stdout.flush()

if __name__ == '__main__':
    df = pd.DataFrame(columns=['file', 'title', 'url', 'hash'])
    df_all = pd.DataFrame(columns=['file', 'title', 'url', 'hash'])

    pool = mp.Pool(processes=mp.cpu_count())

    for i in range(0, 245):
        current_url = base_url + '/list.php?page=%s' % i
        print(current_url)

        code = requests.get(current_url)
        parsed_html = bs(code.text, 'html.parser')
        a_list = parsed_html.findAll('a')

        dirname = str(i).zfill(3)
        os.makedirs(os.path.join(target_path, dirname), exist_ok=True)

        results = []
        df = pd.DataFrame(columns=['file', 'title', 'url', 'hash'])

        for link in a_list:
            link_url = link.get('href')
            link_title = link.get('title')

            result = pool.apply_async(process, (link_url, link_title, dirname), callback=cb)

            results.append(result)

        for r in results:
            r.wait()

        df.to_csv(os.path.join(target_path, dirname, 'metadata.csv'), index=False, header=False)

    df_all.to_csv(os.path.join(target_path, 'metadata.csv'), index=False, header=False)
