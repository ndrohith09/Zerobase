import strawberry
import uvicorn
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import typing 
import psycopg2
import os
from fastapi.middleware.cors import CORSMiddleware

try:
    conn = psycopg2.connect(
        host=os.environ.get('PG_HOST'),
        port= os.environ.get('PG_PORT'),
        database=os.environ.get('PG_DATABASE'),
        user=os.environ.get('PG_USER'),
        password=os.environ.get('PG_PASSWORD')
    )

    # reconnect if connection is closed
    conn.autocommit = True
    cursor = conn.cursor()

    print("Connected to the PostgreSQL database")

except (Exception, psycopg2.DatabaseError) as error:
    print("Cannot connect to the PostgreSQL database")
    print(error)  

#*main
def main(): 
    cursor.execute('CREATE TABLE IF NOT EXISTS Sample (id SERIAL PRIMARY KEY, word VARCHAR(255))') 
    conn.commit() 
    print("Table created successfully")

#*Dataclasses
@strawberry.type
class Sample:
    id : str 
    word : str

@strawberry.type
class Query:
    #*graphquery     

    @strawberry.field
    def all_sample(self) -> typing.List[Sample]:
        cursor.execute("SELECT * FROM Sample")
        lst = cursor.fetchall()
        sample = []
        for i in lst:
            sample.append(Sample(id=i[0], word=i[1]))
        return sample

@strawberry.type
class Mutation:
    #*graphmutation 

    @strawberry.mutation
    def create_sample(self, word: str) -> Sample:
        print(word , type(word))
        cursor.execute("INSERT INTO Sample (word) VALUES (%s) RETURNING id", (word,))
        conn.commit()
        samples_id = cursor.fetchone()[0]
        return Sample(id=samples_id,word=word)

schema = strawberry.Schema(Query , Mutation)


graphql_app = GraphQLRouter(
    schema,
)


app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"], 
)

if __name__ == "__main__": 
    try:
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 
    uvicorn.run(app, host='0.0.0.0', port=8000)