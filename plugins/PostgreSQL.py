import psycopg2
from psycopg2.errors import UniqueViolation


class PostgreSql:
    """
    Класс с основными SQL-методами
    """

    def __init__(self, dbname, host, user, password, port):
        self.connection = psycopg2.connect(
            dbname=dbname,
            host=host,
            user=user,
            password=password,
            port=port
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query: str):
        """
        Функция для создания таблиц и удаления данных из них
        """
        self.cursor.execute(query)
        self.cursor.connection.commit()

    def insert_into_rockets(self, query: str, item_id: int, is_active: bool, cost_per_launch: int, 
                first_flight: str, country: str, company: str, height_m: float, 
                diameter_m: float, mass_kg: float, description: str, load_date: str):
                
        """
        Функция для добавления данных в таблицу spacex.rockets
        """
        try:
            self.cursor.execute(query, (item_id, is_active, cost_per_launch, 
                    first_flight, country, company, height_m, 
                    diameter_m, mass_kg, description, load_date))
            self.cursor.connection.commit()
        except UniqueViolation as e:
            self.cursor.connection.commit()
            pass

    