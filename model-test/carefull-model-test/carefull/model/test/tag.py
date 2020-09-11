from dataclasses import dataclass
from typing import List


@dataclass
class Tag:
    type: str
    user_id: str
    transaction_id: str
    name: str
    value: str

@dataclass
class User:
    type: str
    user_id: str


class TagRepo:

    user_sql = '''
        select distinct type, user_id
        from tag
        where name = %s
    '''

    by_user_sql = '''
        SELECT type, user_id, transaction_id, name, value
          FROM tag
         WHERE user_id = %s
           AND name = %s
           AND value is not null
    '''

    by_name_sql = '''
        SELECT type, user_id, transaction_id, name, value
          FROM tag
         WHERE name = %s
         and in_model = %s
    '''

    by_transaction_sql = '''
        SELECT type, user_id, transaction_id, name, value
          FROM tag
         WHERE transaction_id = %s
           AND value is not null
    '''

    def __init__(self, connection) -> None:
        super().__init__()
        self.connection = connection

    def find_tags_by_user_id_and_name(self, user_id, name) -> List[Tag]:
        cursor = self.connection.cursor()
        cursor.execute(self.by_user_sql, (user_id, name))
        tags = [Tag(*tag) for tag in cursor.fetchall()]
        cursor.close()
        return tags

    def find_all_tags_by_name(self, name, from_date=None, in_model=False) -> List[Tag]:
        cursor = self.connection.cursor()
        sql = self.by_name_sql
        params = (name, in_model)
        if from_date is not None:
            sql += "and created_at >= %s"
            params += (from_date, )
        cursor.execute(sql, params)
        tags = [Tag(*tag) for tag in cursor.fetchall()]
        cursor.close()
        return tags

    def find_by_transaction_id(self, t_id) -> Tag:
        cursor = self.connection.cursor()
        cursor.execute(self.by_transaction_sql, (t_id,))
        tag = Tag(*cursor.fetchone())
        cursor.close()
        return tag

    def find_users_by_tag_name(self, name) -> List[User]:
        cursor = self.connection.cursor()
        cursor.execute(self.user_sql, (name,))
        users = [User(*user) for user in cursor.fetchall()]
        cursor.close()
        return users
