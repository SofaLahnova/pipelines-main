import psycopg2
import csv

class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        conn = psycopg2.connect(dbname='example_bd', user='postgres', password='2143', host='localhost')
        cursor = conn.cursor()

        sql = "SELECT * FROM " + self.table+";" 
  
        cursor.execute(sql)
        data = [(desc[0] for desc in cursor.description)]
        
        cursor.execute("SELECT * FROM " + self.table)
        for row in cursor:
            data.append(row)
                    
        myFile = open(self.output_file + '.csv', 'w', newline='')
        with myFile:
            writer = csv.writer(myFile)
            writer.writerows(data)

        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        data = []
        i=0
        # считываем данные из файла
        with open(self.input_file, newline='') as File:  
            reader = csv.reader(File)
            for row in reader:
                # первая строка это названия столбцов
                if i == 0 : column = tuple(row)             
                else:
                    temp = tuple(row)
                    data.append(temp)
                i=1
        
        conn = psycopg2.connect(dbname='example_bd', user='postgres', password='2143', host='localhost') 
        cursor = conn.cursor()
        #создаем таблицу
        cr_tb = "CREATE TABLE "+self.table+ "("
        for i in range(len(column)):
            if i != len(column)-1 : 
                cr_tb += column[i]+" text, "
            else : cr_tb += column[i]+" text); " 
        
        cursor.execute(cr_tb)     
        #вставляем данные
        ins_data = "INSERT INTO "+self.table+" VALUES("+"%s,"*(len(column)-1)+" %s);"
        cursor.executemany(ins_data, data)
        conn.commit()
        
        print(f"Load file `{self.input_file}` to table `{self.table}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        print(f"Run SQL ({self.title}):\n{self.sql_query}") 
        conn = psycopg2.connect(dbname='example_bd', user='postgres', password='2143', host='localhost')
        cursor = conn.cursor()
        cursor.execute(self.sql_query)
        conn.commit()
        cursor.close()
        conn.close()
        



class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

   

    def run(self):
        conn = psycopg2.connect(dbname='example_bd', user='postgres', password='2143', host='localhost')
        cursor = conn.cursor()
        # создаем хранимую процкдуру для определения домена 
        postgresql_func = """
            CREATE OR REPLACE FUNCTION domain_of_url(x text)
              RETURNS text AS 
              $$
            BEGIN
             RETURN split_part(split_part(x, '://', 2), '/', 1) ;            
            END;
              $$
            LANGUAGE PLPGSQL;
            """
 
        cursor.execute(postgresql_func)
        conn.commit()
        cursor.execute("Create table " + self.table + " as " + self.sql_query + "")
        conn.commit()
        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")

