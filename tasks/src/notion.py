import jmespath
import numpy as np
import pandas as pd
from requests import Session
from requests.auth import AuthBase

from core.aws import ECSOperator, SSMOperator


class Bearer(AuthBase):
    def __init__(self):
        ssm = SSMOperator()
        self.token = ssm.get_parameter('/api_key/notion/alwaysdodo', with_decryption=True)
        print(self.token)

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r


class NotionClient(Session):
    def __init__(self):
        super().__init__()
        self.headers['Content-Type'] = 'application/json'
        self.headers['Notion-Version'] = '2021-08-16'
        self.auth = Bearer()

    def get_recurse_block(self, block_id, cursor=None):
        next_cursor = cursor
        while True:
            uri = f'https://api.notion.com/v1/blocks/{block_id}/children?page_size=10'
            if next_cursor is not None:
                uri += f'&start_cursor={next_cursor}'
            response = self.get(uri)
            if response.status_code != 200:
                raise RuntimeError
            payload = response.json()
            if payload['object'] == 'list':
                for block in payload['results']:
                    if block['has_children']:
                        yield from self.get_recurse_block(block['id'], cursor=None)
                    else:
                        yield block | {'parent': block_id}
            if not payload['has_more']:
                break
            next_cursor = payload['next_cursor']

    def query_database(self, database_id, data):
        uri = f"https://api.notion.com/v1/databases/{database_id}/query"
        return self.post(uri, json=data)


def get_all_child_table():
    result = []
    client = NotionClient()
    for block in client.get_recurse_block('494c1b3052dc404fb4bdf06fbc90c4e1'):
        # print(block['id'], block['type'])
        if block['type'] == 'child_database':
            response = client.query_database(block['id'], None)
            if response.status_code == 404:
                continue
            data = response.json() | {'parent': block['id']}
            if data['object'] == 'error':
                print(data)
                continue
            result.append(data)
    return result


def get_value(x):
    if isinstance(x, dict):
        t = x['type']
        return jmespath.search('[0].text.content', x[t])
    else:
        return x


def aggregate(series: pd.Series):
    counts = series.str.findall(r'(\d+)/(\d+)')
    return counts.apply(lambda x: sum(np.asarray(x, dtype=int)))


def main():
    frame = get_all_child_table()
    data = jmespath.search('[].{parent: parent, properties: results[].properties}', frame)
    prop = [{'parent': d['parent']} | p for d in data for p in d['properties']]
    df = pd.DataFrame(prop)
    parsed = df.applymap(get_value)
    weeks = ['1주차', '2주차', '3주차']
    result = parsed.dropna(subset=['목표', *weeks], how='any')
    result.loc[:, weeks] = result.loc[:, weeks].apply(aggregate, axis=1)
    return result


def task_def():
    ecs = ECSOperator('jongwony', 'tasks', 'container')
    ecs.register_task_definition(
        command=['python', '/tmp/src/notion.py'],
        image='237943334087.dkr.ecr.ap-northeast-2.amazonaws.com/tasks:latest',
        cpu=256,
        memory=512,
    )


def run():
    ecs = ECSOperator('jongwony', 'tasks', 'container')
    ecs.run_task()


if __name__ == '__main__':
    results = main()
    weeks = ['1주차', '2주차', '3주차']
    results[weeks].sum()
    results.groupby('parent')[weeks].sum()
    # task_def()
    # run()
