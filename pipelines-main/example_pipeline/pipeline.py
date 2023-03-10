from pipelines import tasks, Pipeline


NAME = 'test_project'
VERSION = '2023'


TASKS = [
    tasks.LoadFile(input_file='original/original.csv', table='original'),
    tasks.CTAS(
        table='norm',
        sql_query='''
            select *, domain_of_url(url)
            from {original};
        '''
    ),
    tasks.CopyToFile(
        table='norm',
        output_file='norm',
    ),
    tasks.RunSQL('''CREATE TABLE original
                          (ID INT PRIMARY KEY     NOT NULL,
                           name          TEXT    NOT NULL,
                           url         text NOT NULL ); '''),

    # clean up:

    tasks.RunSQL("insert INTO original VALUES(1,'hello','http://hello.com/home');"),

    #tasks.RunSQL('drop table original;'),
    #tasks.RunSQL('drop table norm;'),
    

]


pipeline = Pipeline(
    name=NAME,
    version=VERSION,
    tasks=TASKS
)


if __name__ == "__main__":
    # 1: Run as script
    pipeline.run()

    # 2: Run as CLI
    # > pipelines run
