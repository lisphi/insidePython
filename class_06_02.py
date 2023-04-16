import json
import os


def main():
    params = {
        'symbol': '123456',
        'type': 'limit',
        'price': 123.4,
        'amount': 23
    }
    print(f'type of params = {type(params)}, params = {params}')

    params_str = json.dumps(params)
    print(f'type of params_str = {type(params_str)}, params_str = {params_str}')

    origin_params1 = json.loads(params_str)
    print(f'type of origin_params1 = {type(origin_params1)}, origin_params1 = {origin_params1}')

    with open(os.path.join(os.getcwd(), 'class_06_02_params.json'), 'w') as fout:
        json.dump(params, fout)

    with open(os.path.join(os.getcwd(), 'class_06_02_params.json'), 'r') as fin:
        origin_params2 = json.load(fin)

    print(f'type of origin_params2 = {type(origin_params2)}, origin_params2 = {origin_params2}')


if __name__ == '__main__':
    main()