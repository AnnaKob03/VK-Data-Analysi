import requests
import time
import logging
import argparse
from neo4j import GraphDatabase

def vk_api_request(method, token, params, retries=3, delay=2, use_service_token=False, service_token=None):
    url = f"https://api.vk.com/method/{method}"
    if use_service_token and service_token:
        params['access_token'] = service_token
    else:
        params['access_token'] = token
    params['v'] = '5.131'

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if 'error' in data:
                if data['error']['error_code'] == 30:  # Приватный аккаунт
                    logging.warning(f"Профиль {params.get('user_id')} является приватным.")
                    return {'is_private': True}
                else:
                    raise Exception(f"API Error: {data['error']['error_msg']}")
            return data['response']
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP ошибка: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка запроса: {e}")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка: {e}")

        logging.info(f"Повтор запроса через {delay} секунд...")
        time.sleep(delay)

    raise Exception("Повторные попытки не дали результата: сервер не отвечает")


def process_profile(profile):
    name = f"{profile.get('first_name', '')} {profile.get('last_name', '')}".strip()
    home_town = profile.get('home_town', '')
    if not home_town and 'city' in profile and 'title' in profile['city']:
        home_town = profile['city']['title']

    return {
        'id': profile['id'],
        'screen_name': profile.get('screen_name', ''),
        'name': name,
        'sex': profile.get('sex', 0),
        'home_town': home_town,
        'is_private': 'deactivated' in profile or (profile.get('is_closed') and not profile.get('can_access_closed'))
    }


class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logging.info("Подключение к базе данных Neo4j установлено.")

    def close(self):
        self.driver.close()
        logging.info("Соединение с Neo4j закрыто.")

    def create_user(self, user_data):
        try:
            with self.driver.session() as session:
                session.write_transaction(self._create_user_node, user_data)
        except Exception as e:
            logging.error(f"Ошибка при создании узла пользователя {user_data['id']}: {e}")

    @staticmethod
    def _create_user_node(tx, user_data):
        tx.run(
            """
            MERGE (u:User {id: $id})
            SET u.screen_name = $screen_name,
                u.name = $name,
                u.sex = $sex,
                u.home_town = $home_town,
                u.friends_count = $friends_count,
                u.subscriptions_count = $subscriptions_count
            """,
            id=user_data['id'],
            screen_name=user_data['screen_name'],
            name=user_data['name'],
            sex=user_data['sex'],
            home_town=user_data['home_town'],
            friends_count=user_data.get('friends_count', 0),
            subscriptions_count=user_data.get('subscriptions_count', 0)
        )

    def create_group(self, group_data):
        try:
            with self.driver.session() as session:
                session.write_transaction(self._create_group_node, group_data)
        except Exception as e:
            logging.error(f"Ошибка при создании узла группы {group_data['id']}: {e}")

    @staticmethod
    def _create_group_node(tx, group_data):
        tx.run(
            """
            MERGE (g:Group {id: $id})
            SET g.name = $name,
                g.members_count = $members_count
            """,
            id=group_data['id'],
            name=group_data['name'],
            members_count=group_data.get('members_count', 0)
        )

    def create_friendship(self, user_id1, user_id2):
        try:
            with self.driver.session() as session:
                session.write_transaction(self._create_friendship_relation, user_id1, user_id2)
        except Exception as e:
            logging.error(f"Ошибка при создании связи FRIEND между {user_id1} и {user_id2}: {e}")

    @staticmethod
    def _create_friendship_relation(tx, user_id1, user_id2):
        tx.run(
            """
            MATCH (u1:User {id: $user_id1}), (u2:User {id: $user_id2})
            MERGE (u1)-[:FRIEND]->(u2)
            """,
            user_id1=user_id1,
            user_id2=user_id2
        )

    def create_subscription(self, user_id, group_id):
        try:
            with self.driver.session() as session:
                session.write_transaction(self._create_subscription_relation, user_id, group_id)
        except Exception as e:
            logging.error(f"Ошибка при создании связи SUBSCRIBED_TO между {user_id} и группой {group_id}: {e}")

    @staticmethod
    def _create_subscription_relation(tx, user_id, group_id):
        tx.run(
            """
            MATCH (u:User {id: $user_id}), (g:Group {id: $group_id})
            MERGE (u)-[:SUBSCRIBED_TO]->(g)
            """,
            user_id=user_id,
            group_id=group_id
        )

    def execute_query(self, query, parameters=None):
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters)
                return [record for record in result]
        except Exception as e:
            logging.error(f"Ошибка при выполнении запроса: {e}")
            return []


def get_user_data(token, service_token, user_id, db_handler, friends_limit=20, subscriptions_limit=40):
    try:
        user_info_data = vk_api_request(
            'users.get', token,
            {'user_ids': user_id, 'fields': 'screen_name,sex,home_town,city,first_name,last_name'}
        )[0]
        user_info = process_profile(user_info_data)
    except Exception as e:
        logging.error(f"Ошибка при получении информации о пользователе {user_id}: {e}")
        return None

    # Инициализируем счетчики
    friends_count = 0
    subscriptions_count = 0

    # Получаем друзей
    try:
        friends_fields = 'screen_name,sex,home_town,city,first_name,last_name'
        friends = vk_api_request(
            'friends.get', token,
            {'user_id': user_id, 'fields': friends_fields, 'count': friends_limit}
        )

        if isinstance(friends, dict):
            friends_list = friends.get('items', [])
            friends_count = len(friends_list)
            for friend in friends_list:
                friend_profile = process_profile(friend)
                if not friend_profile['is_private']:
                    db_handler.create_user(friend_profile)
                    db_handler.create_friendship(user_info['id'], friend_profile['id'])
                    logging.info(f"Друг {friend_profile['id']} сохранен и связь FRIEND создана.")
        else:
            logging.warning("Непредвиденная структура ответа для метода friends.get")
            friends_count = 0
    except Exception as e:
        logging.error(f"Ошибка при получении друзей пользователя {user_id}: {e}")
        friends_count = 0

    # Получаем подписки (только группы)
    try:
        offset = 0
        count_per_request = 200

        all_group_ids = []

        while True:
            subscriptions = vk_api_request(
                'users.getSubscriptions', token,
                {
                    'user_id': user_id,
                    'extended': 1,
                    'offset': offset,
                    'count': count_per_request,
                    'filter': 'groups'
                }
            )

            if not isinstance(subscriptions, dict):
                logging.warning("Непредвиденная структура ответа для метода users.getSubscriptions")
                break

            items = subscriptions.get('items', [])
            total_subscriptions = subscriptions.get('count', 0)
            subscriptions_count = total_subscriptions

            # Собираем ID групп
            group_ids = [str(sub['id']) for sub in items if sub.get("is_closed", 0) == 0]
            all_group_ids.extend(group_ids)

            if len(items) < count_per_request or len(all_group_ids) >= subscriptions_limit:
                break

            offset += count_per_request

        # Ограничиваем общее количество групп по subscriptions_limit
        all_group_ids = all_group_ids[:subscriptions_limit]

        # Получаем информацию о группах через groups.getById с использованием сервисного токена
        if all_group_ids:
            group_chunks = [all_group_ids[i:i + 500] for i in range(0, len(all_group_ids), 500)]
            for chunk in group_chunks:
                groups_info = vk_api_request(
                    'groups.getById', token,
                    {
                        'group_ids': ','.join(chunk),
                        'fields': 'members_count'
                    },
                    use_service_token=True,
                    service_token=service_token
                )

                if not isinstance(groups_info, list):
                    logging.error(f"Ошибка при получении информации о группах: {groups_info}")
                    continue

                for group in groups_info:
                    group_data = {
                        'id': group['id'],
                        'name': group.get('name', ''),
                        'members_count': group.get('members_count', 0)
                    }
                    logging.info(f"Сохраняем группу: {group_data}")
                    db_handler.create_group(group_data)
                    db_handler.create_subscription(user_info['id'], group_data['id'])
                    logging.info(f"Подписка на группу {group_data['id']} сохранена.")

    except Exception as e:
        logging.error(f"Ошибка при получении подписок пользователя {user_id}: {e}")
        subscriptions_count = 0

    # Обновляем информацию о пользователе с количеством друзей и подписок
    user_info['friends_count'] = friends_count
    user_info['subscriptions_count'] = subscriptions_count

    # Сохраняем узел пользователя с обновленными данными
    db_handler.create_user(user_info)
    logging.info(f"Информация о пользователе {user_info['id']} сохранена с количеством друзей и подписок.")


def get_detailed_data(token, service_token, user_id, db_handler, depth_limit, current_depth=1, friends_limit=20,
                      subscriptions_limit=40, visited_users=None):
    if visited_users is None:
        visited_users = set()

    if user_id in visited_users:
        return
    visited_users.add(user_id)

    get_user_data(token, service_token, user_id, db_handler, friends_limit, subscriptions_limit)

    if current_depth < depth_limit:
        # Получаем друзей для дальнейшей обработки
        try:
            friends_fields = 'screen_name,sex,home_town,city,first_name,last_name'
            friends = vk_api_request(
                'friends.get', token,
                {'user_id': user_id, 'fields': friends_fields, 'count': friends_limit}
            )

            if isinstance(friends, dict):
                friends_list = friends.get('items', [])
                for friend in friends_list:
                    friend_profile = process_profile(friend)
                    if not friend_profile['is_private']:
                        get_detailed_data(
                            token, service_token, friend_profile['id'], db_handler, depth_limit, current_depth + 1,
                            friends_limit, subscriptions_limit, visited_users
                        )
            else:
                logging.warning("Непредвиденная структура ответа для метода friends.get")
        except Exception as e:
            logging.error(f"Ошибка при рекурсивном получении друзей пользователя {user_id}: {e}")


def execute_queries(db_handler, query_type=None):
    # Если query_type не указан, выполняем все запросы
    queries_to_execute = []
    if query_type:
        queries_to_execute.append(query_type)
    else:
        queries_to_execute = ['total_users', 'total_groups', 'top_groups', 'top_users', 'mutual_friends']

    for q_type in queries_to_execute:
        if q_type == 'total_users':
            query = "MATCH (u:User) RETURN count(u) as total_users"
            result = db_handler.execute_query(query)
            if result:
                total_users = result[0]['total_users']
                print(f"Всего пользователей: {total_users}")
        elif q_type == 'total_groups':
            query = "MATCH (g:Group) RETURN count(DISTINCT g) as total_groups"
            result = db_handler.execute_query(query)
            if result:
                total_groups = result[0]['total_groups']
                print(f"Всего групп: {total_groups}")
        elif q_type == 'top_users':
            query = """
            MATCH (u:User)-[:FRIEND]->(f:User)
            RETURN u.id as user_id, u.name as name, count(f) as friends_count
            ORDER BY friends_count DESC
            LIMIT 5
            """
            result = db_handler.execute_query(query)
            print("\nТоп 5 пользователей по количеству друзей:")
            for record in result:
                print(f"ID: {record['user_id']}, Имя: {record['name']}, Количество друзей: {record['friends_count']}")
        elif q_type == 'top_groups':
            query = """
            MATCH (g:Group)
            WHERE g.members_count IS NOT NULL
            RETURN g.id as group_id, g.name as name, g.members_count as members_count
            ORDER BY g.members_count DESC
            LIMIT 5
            """
            result = db_handler.execute_query(query)
            print("\nТоп 5 групп по количеству участников:")
            for record in result:
                print(f"ID: {record['group_id']}, Название: {record['name']}, Количество участников: {record['members_count']}")
        elif q_type == 'mutual_friends':
            query = """
            MATCH (u1:User)-[:FRIEND]->(u2:User)
            WHERE (u2)-[:FRIEND]->(u1) AND u1.id < u2.id
            RETURN u1.id as user1_id, u1.name as user1_name, u2.id as user2_id, u2.name as user2_name
            """
            result = db_handler.execute_query(query)
            print("\nПары пользователей, которые являются друзьями друг друга:")
            for record in result:
                print(f"{record['user1_name']} (ID: {record['user1_id']}) и {record['user2_name']} (ID: {record['user2_id']})")
        else:
            print("Неверный тип запроса.")


def clear_database(db_handler):
    with db_handler.driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    logging.info("База данных Neo4j очищена.")


def main():
    # Настройка логирования
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='VK Data Collector and Neo4j Analyzer')
    parser.add_argument('--query', type=str, choices=['total_users', 'total_groups', 'top_users', 'top_groups', 'mutual_friends'],
                        help='Type of query to execute on the database')
    parser.add_argument('--depth_limit', type=int, default=3, help='Depth limit for data collection')
    parser.add_argument('--friends_limit', type=int, default=100, help='Friends limit per user')
    parser.add_argument('--subscriptions_limit', type=int, default=300, help='Subscriptions limit per user')
    parser.add_argument('--uri', type=str, default='bolt://localhost:7687', help='Neo4j URI')
    args = parser.parse_args()

    # Интерактивный ввод параметров
    token = input("Введите токен доступа VK: ")
    service_token = input("Введите сервисный токен VK: ")
    user_id = input("Введите VK User ID или Screen Name: ")

    db_user = input("Введите имя пользователя базы данных Neo4j (по умолчанию 'neo4j'): ") or 'neo4j'
    db_password = input("Введите пароль базы данных Neo4j: ")

    if not token or not user_id or not service_token:
        print('Необходимо ввести токен, сервисный токен и ID пользователя VK для сбора данных.')
        return

    try:
        db_handler = Neo4jHandler(args.uri, db_user, db_password)

        # Очистка базы данных перед сбором новых данных
        clear_database(db_handler)

        if args.query:
            # Выполняем запросы без сбора данных
            execute_queries(db_handler, args.query)
        else:
            # Сбор данных из VK и сохранение в базу данных
            get_detailed_data(token, service_token, user_id, db_handler, args.depth_limit, friends_limit=args.friends_limit,
                              subscriptions_limit=args.subscriptions_limit)
            logging.info("Данные успешно сохранены в базу данных Neo4j.")

            # Выполнение запросов после сохранения данных
            execute_queries(db_handler)

    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
    finally:
        db_handler.close()

if __name__ == '__main__':
    main()
