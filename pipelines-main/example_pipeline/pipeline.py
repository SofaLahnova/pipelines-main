from pipelines import tasks, Pipeline


NAME = 'test_project'
VERSION = '2023'


TASKS = [
    tasks.RunSQL('drop table original;'),
    #tasks.RunSQL('drop table norm;'),
    
    tasks.RunSQL('''CREATE TABLE original
                          (ID INT PRIMARY KEY     NOT NULL,
                          name          TEXT    NOT NULL,
                          url         text NOT NULL ); '''),

    tasks.LoadFile(input_file='original.csv', table='original'),
    #tasks.RunSQL("insert INTO original VALUES(1,'hello','http://hello.com/home');"),




    tasks.CTAS( 
        table='norm',
        sql_query='''
            select *, domain_of_url(url)
            from original;
        '''
    ),
   #tasks.RunSQL('''CREATE TABLE norm as select * from original; '''),


    tasks.CopyToFile(
        table='norm',
        output_file='norm',
    ),
    

    # clean up:

    #tasks.RunSQL("insert INTO original VALUES(10,'hello','http://hello.com/home');"),

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
