import requests
from faker import Faker
from copy import deepcopy
from dataclasses import dataclass

@dataclass
class Agent:
    ip: dict
    token: str

ua = Faker()

PAYLOAD_TEMPLATE = {
    'Id': '',
    'context': {
        'tourToken': '',
    },
}

def initAgent(agent: Agent):
    HEADERS = {
        'accept': 'application/json',
        'accept-language': 'zh-CN,zh;q=0.9',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://g2.ltfc.net',
        'referer': 'https://g2.ltfc.net/',
        'user-agent': ua.user_agent()
    }
    response = requests.post('https://api.quanku.art/cag2.TouristService/getAccessToken', headers=HEADERS, proxies=agent.ip)
    agent.token = response.json()['token']

def getPayload(agent: Agent, id: str):
    PAYLOAD = deepcopy(PAYLOAD_TEMPLATE)
    PAYLOAD["context"]["tourToken"] = agent.token
    PAYLOAD["Id"] = id
    return PAYLOAD

def getArticle(agent, id: str):
    PAYLOAD, HEADERS = getPayload(agent, id)
    response = requests.post('https://api.quanku.art/cag2.ArtistService/get', headers=HEADERS, json=PAYLOAD, proxies=agent.ip)
    return response.json()

def main():
    agent = Agent(ip={}, token='')
    initAgent(agent)

    res = getArticle(agent, '5df8a8c15e3be25e694d7130')
    print(res)

if __name__ == '__main__':
    main()
