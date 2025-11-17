import requests
from generateDetailUrl import fetch_all_tile

headers = {
    'accept': 'application/json',
    'accept-language': 'zh-CN,zh;q=0.9',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://g2.ltfc.net',
    'referer': 'https://g2.ltfc.net/',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
}

json_data = {
    'Id': '5df8a8c05e3be25e694d712f',
    'page': {
        'skip': 0,
        'limit': 999,
    },
    'context': {
        'tourToken': 'aRV4N20Y3DZxpgD5Zp73Ziy.4WEk/SJQvdzmW3ZFTYIxiw==.t',
    },
}



response = requests.post('https://api.quanku.art/cag2.ArtistService/listHuiaOfArtist', headers=headers, json=json_data)

for item in response.json()['data']:
    id = item['Id']
    json_data2 = {
        'src': 'SUHA',
        'id': id,
        'context': {
            'tourToken': 'aRV4N20Y3DZxpgD5Zp73Ziy.4WEk/SJQvdzmW3ZFTYIxiw==.t',
        },
    }
    print(id)
    response = requests.post(f'https://api.quanku.art/cag2.ResourceService/getSubList', headers=headers, json=json_data2)
    for tm in response.json()["data"]:
        resourceID = tm['suha']['Id']
        json_data3 = {
            'id': f'{resourceID}',
            'src': 'SUHA',
            'context': {
                'tourToken': 'aRV4N20Y3DZxpgD5Zp73Ziy.4WEk/SJQvdzmW3ZFTYIxiw==.t',
            },
        }
        response = requests.post('https://api.quanku.art/cag2.ResourceService/getResource', headers=headers, json=json_data3)
        print(response.json())
        resourceID = response.json()["data"]["suha"]["hdp"]["hdpic"]["resourceId"]
        print(resourceID)
        fetch_all_tile(resourceID)

    exit(0)
