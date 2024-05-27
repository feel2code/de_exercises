# прекод для выполнения задания
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма

book = [('Harry Potter and the Goblet of Fire', 'J. K. Rowling', 322),
        ('Nineteen Eighty-Four', 'George Orwell', 382),
        ('Jane Eyre', 'Charlotte Brontë', 159),
        ('Catch-22', 'Joseph Heller',  174),
        ('The Catcher in the Rye', 'J. D. Salinger',  168),
        ('The Wind in the Willows', 'Kenneth Grahame',  259),
        ('The Mayor of Casterbridge', 'Thomas Hardy',  300),
        ('Bad Girls', 'Jacqueline Wilson',  299)
]

# данные второго датафрейма
library = [
        ( 322, "1"),
        ( 250, "2" ),
        (400, "2"),
        (159, "1"),
        (382, "2"),
        (322, "1")
]

# названия атрибутов
columns = ['title', 'author', 'book_id']
columns_library = ['book_id', 'Library_id']

# создаём датафреймы
df = spark.createDataFrame(data=book, schema=columns)
df_library  = spark.createDataFrame(data=library, schema=columns_library )

# делаем join
df_join = df.join(df_library,['book_id'], 'leftanti').select('title')
df_cache= df_join.cache()

spark.sparkContext.setCheckpointDir("/home/USER/test")
df_cache.checkpoint()
df_cache.show()
df_cache.explain()
